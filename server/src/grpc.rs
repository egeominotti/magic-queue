//! gRPC server implementation for flashQ
//!
//! Provides high-performance streaming API for job processing.

use std::pin::Pin;
use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, Stream, StreamExt};
use tonic::{Request, Response, Status, Streaming};

use crate::protocol::{
    Job as InternalJob, JobInput as InternalJobInput, JobState as InternalJobState,
};
use crate::queue::QueueManager;

// Include generated protobuf code
pub mod pb {
    tonic::include_proto!("flashq");
}

use pb::queue_service_server::{QueueService, QueueServiceServer};
use pb::*;

/// gRPC service implementation
pub struct QueueServiceImpl {
    queue_manager: Arc<QueueManager>,
}

impl QueueServiceImpl {
    pub fn new(queue_manager: Arc<QueueManager>) -> Self {
        Self { queue_manager }
    }

    pub fn into_server(self) -> QueueServiceServer<Self> {
        QueueServiceServer::new(self)
    }
}

// Helper conversions
impl From<InternalJob> for Job {
    fn from(job: InternalJob) -> Self {
        Job {
            id: job.id,
            queue: job.queue,
            data: serde_json::to_vec(&job.data).unwrap_or_default(),
            priority: job.priority,
            created_at: job.created_at,
            run_at: job.run_at,
            started_at: job.started_at,
            attempts: job.attempts,
            max_attempts: job.max_attempts,
            backoff: job.backoff,
            ttl: job.ttl,
            timeout: job.timeout,
            unique_key: job.unique_key,
            depends_on: job.depends_on,
            progress: job.progress as u32,
            progress_msg: job.progress_msg,
            lifo: job.lifo,
        }
    }
}

impl From<InternalJobState> for JobState {
    fn from(state: InternalJobState) -> Self {
        match state {
            InternalJobState::Waiting => JobState::Waiting,
            InternalJobState::Delayed => JobState::Delayed,
            InternalJobState::Active => JobState::Active,
            InternalJobState::Completed => JobState::Completed,
            InternalJobState::Failed => JobState::Failed,
            InternalJobState::WaitingChildren => JobState::WaitingChildren,
            InternalJobState::WaitingParent => JobState::WaitingChildren, // Map to same gRPC enum
            InternalJobState::Stalled => JobState::Active, // Map stalled to active for gRPC
            InternalJobState::Unknown => JobState::Unknown,
        }
    }
}

#[tonic::async_trait]
impl QueueService for QueueServiceImpl {
    // === Unary RPCs ===

    async fn push(&self, request: Request<PushRequest>) -> Result<Response<PushResponse>, Status> {
        let req = request.into_inner();

        let data: serde_json::Value = serde_json::from_slice(&req.data)
            .map_err(|e| Status::invalid_argument(format!("Invalid JSON data: {}", e)))?;

        match self
            .queue_manager
            .push(
                req.queue,
                data,
                req.priority,
                if req.delay_ms > 0 {
                    Some(req.delay_ms)
                } else {
                    None
                },
                if req.ttl_ms > 0 {
                    Some(req.ttl_ms)
                } else {
                    None
                },
                if req.timeout_ms > 0 {
                    Some(req.timeout_ms)
                } else {
                    None
                },
                if req.max_attempts > 0 {
                    Some(req.max_attempts)
                } else {
                    None
                },
                if req.backoff_ms > 0 {
                    Some(req.backoff_ms)
                } else {
                    None
                },
                req.unique_key,
                if req.depends_on.is_empty() {
                    None
                } else {
                    Some(req.depends_on)
                },
                None, // tags not supported in gRPC yet
                req.lifo,
                false, // remove_on_complete
                false, // remove_on_fail
                None,  // stall_timeout
                None,  // debounce_id
                None,  // debounce_ttl
                None,  // job_id (custom ID)
                None,  // keep_completed_age
                None,  // keep_completed_count
            )
            .await
        {
            Ok(job) => Ok(Response::new(PushResponse {
                ok: true,
                id: job.id,
            })),
            Err(e) => Err(Status::internal(e)),
        }
    }

    async fn push_batch(
        &self,
        request: Request<PushBatchRequest>,
    ) -> Result<Response<PushBatchResponse>, Status> {
        let req = request.into_inner();

        let mut jobs = Vec::with_capacity(req.jobs.len());
        for j in req.jobs {
            let data: serde_json::Value = serde_json::from_slice(&j.data)
                .map_err(|e| Status::invalid_argument(format!("Invalid JSON: {}", e)))?;

            jobs.push(InternalJobInput {
                data,
                priority: j.priority,
                delay: if j.delay_ms > 0 {
                    Some(j.delay_ms)
                } else {
                    None
                },
                ttl: if j.ttl_ms > 0 { Some(j.ttl_ms) } else { None },
                timeout: if j.timeout_ms > 0 {
                    Some(j.timeout_ms)
                } else {
                    None
                },
                max_attempts: if j.max_attempts > 0 {
                    Some(j.max_attempts)
                } else {
                    None
                },
                backoff: if j.backoff_ms > 0 {
                    Some(j.backoff_ms)
                } else {
                    None
                },
                unique_key: j.unique_key,
                depends_on: if j.depends_on.is_empty() {
                    None
                } else {
                    Some(j.depends_on)
                },
                tags: None,
                lifo: j.lifo,
                remove_on_complete: false,
                remove_on_fail: false,
                stall_timeout: None,
                debounce_id: None,
                debounce_ttl: None,
                job_id: None,
                keep_completed_age: None,
                keep_completed_count: None,
            });
        }

        let ids = self.queue_manager.push_batch(req.queue, jobs).await;
        Ok(Response::new(PushBatchResponse { ok: true, ids }))
    }

    async fn pull(&self, request: Request<PullRequest>) -> Result<Response<Job>, Status> {
        let req = request.into_inner();
        let job = self.queue_manager.pull(&req.queue).await;
        Ok(Response::new(job.into()))
    }

    async fn pull_batch(
        &self,
        request: Request<PullBatchRequest>,
    ) -> Result<Response<PullBatchResponse>, Status> {
        let req = request.into_inner();
        // Limit batch size to prevent DOS attacks (max 1000 jobs per request)
        const MAX_BATCH_SIZE: u32 = 1000;
        let count = (req.count as usize).min(MAX_BATCH_SIZE as usize);
        let jobs = self.queue_manager.pull_batch(&req.queue, count).await;
        Ok(Response::new(PullBatchResponse {
            jobs: jobs.into_iter().map(Into::into).collect(),
        }))
    }

    async fn ack(&self, request: Request<AckRequest>) -> Result<Response<AckResponse>, Status> {
        let req = request.into_inner();

        let result = if let Some(data) = req.result {
            Some(
                serde_json::from_slice(&data)
                    .map_err(|e| Status::invalid_argument(format!("Invalid result JSON: {}", e)))?,
            )
        } else {
            None
        };

        match self.queue_manager.ack(req.id, result).await {
            Ok(()) => Ok(Response::new(AckResponse { ok: true })),
            Err(e) => Err(Status::not_found(e)),
        }
    }

    async fn ack_batch(
        &self,
        request: Request<AckBatchRequest>,
    ) -> Result<Response<AckBatchResponse>, Status> {
        let req = request.into_inner();
        let acked = self.queue_manager.ack_batch(&req.ids).await;
        Ok(Response::new(AckBatchResponse {
            ok: true,
            acked: acked as u32,
        }))
    }

    async fn fail(&self, request: Request<FailRequest>) -> Result<Response<FailResponse>, Status> {
        let req = request.into_inner();
        match self.queue_manager.fail(req.id, req.error).await {
            Ok(()) => Ok(Response::new(FailResponse { ok: true })),
            Err(e) => Err(Status::not_found(e)),
        }
    }

    async fn get_state(
        &self,
        request: Request<GetStateRequest>,
    ) -> Result<Response<GetStateResponse>, Status> {
        let req = request.into_inner();
        let state = self.queue_manager.get_state(req.id);
        Ok(Response::new(GetStateResponse {
            id: req.id,
            state: JobState::from(state).into(),
        }))
    }

    async fn stats(
        &self,
        _request: Request<StatsRequest>,
    ) -> Result<Response<StatsResponse>, Status> {
        let (queued, processing, delayed, dlq) = self.queue_manager.stats().await;
        Ok(Response::new(StatsResponse {
            queued: queued as u64,
            processing: processing as u64,
            delayed: delayed as u64,
            dlq: dlq as u64,
        }))
    }

    // === Streaming RPCs ===

    type StreamJobsStream = Pin<Box<dyn Stream<Item = Result<Job, Status>> + Send>>;

    async fn stream_jobs(
        &self,
        request: Request<StreamJobsRequest>,
    ) -> Result<Response<Self::StreamJobsStream>, Status> {
        let req = request.into_inner();
        let queue = req.queue;
        // Limit batch size to prevent DOS attacks
        const MAX_BATCH_SIZE: usize = 100;
        const MAX_PREFETCH: usize = 1000;
        let batch_size = if req.batch_size > 0 {
            (req.batch_size as usize).min(MAX_BATCH_SIZE)
        } else {
            1
        };
        let qm = Arc::clone(&self.queue_manager);

        let (tx, rx) = mpsc::channel((req.prefetch as usize).clamp(1, MAX_PREFETCH));

        // Spawn background task to pull jobs and send to stream
        // Uses timeout to periodically check if client disconnected
        tokio::spawn(async move {
            loop {
                // Check if client disconnected before blocking on pull
                if tx.is_closed() {
                    break;
                }

                if batch_size == 1 {
                    // Use timeout to periodically check for client disconnect
                    let pull_result =
                        tokio::time::timeout(tokio::time::Duration::from_secs(30), qm.pull(&queue))
                            .await;

                    match pull_result {
                        Ok(job) => {
                            if tx.send(Ok(job.into())).await.is_err() {
                                break; // Client disconnected
                            }
                        }
                        Err(_) => {
                            // Timeout - check if client still connected and retry
                            if tx.is_closed() {
                                break;
                            }
                        }
                    }
                } else {
                    let jobs = qm.pull_batch(&queue, batch_size).await;
                    for job in jobs {
                        if tx.send(Ok(job.into())).await.is_err() {
                            break;
                        }
                    }
                    // Small delay to prevent busy loop when queue is empty
                    if tx.is_closed() {
                        break;
                    }
                }
            }
        });

        let stream = ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(stream)))
    }

    type ProcessJobsStream = Pin<Box<dyn Stream<Item = Result<Job, Status>> + Send>>;

    async fn process_jobs(
        &self,
        request: Request<Streaming<JobResult>>,
    ) -> Result<Response<Self::ProcessJobsStream>, Status> {
        let mut inbound = request.into_inner();
        let qm = Arc::clone(&self.queue_manager);
        let qm2 = Arc::clone(&self.queue_manager);

        let (tx, rx) = mpsc::channel(32);

        // Task to process incoming results from client
        tokio::spawn(async move {
            while let Some(result) = inbound.next().await {
                match result {
                    Ok(job_result) => {
                        if job_result.success {
                            let result_data = job_result
                                .result
                                .and_then(|d| serde_json::from_slice(&d).ok());
                            let _ = qm.ack(job_result.id, result_data).await;
                        } else {
                            let _ = qm.fail(job_result.id, job_result.error).await;
                        }
                    }
                    Err(_) => break,
                }
            }
        });

        // Task to send jobs to client
        // NOTE: This bidirectional stream uses "default" queue. For production use,
        // clients should use the unidirectional stream_jobs() RPC which accepts a queue parameter,
        // or use the HTTP/WebSocket API for queue-specific streaming.
        tokio::spawn(async move {
            let queue = "default".to_string();
            loop {
                let job = qm2.pull(&queue).await;
                if job.id == 0 {
                    // No job available, wait a bit
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                    continue;
                }
                if tx.send(Ok(job.into())).await.is_err() {
                    break; // Client disconnected
                }
            }
        });

        let stream = ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(stream)))
    }
}

/// Create and run the gRPC server
pub async fn run_grpc_server(
    addr: std::net::SocketAddr,
    queue_manager: Arc<QueueManager>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let service = QueueServiceImpl::new(queue_manager);

    println!("gRPC server listening on {}", addr);

    tonic::transport::Server::builder()
        .add_service(service.into_server())
        .serve(addr)
        .await?;

    Ok(())
}
