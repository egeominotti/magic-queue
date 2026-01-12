<?php

declare(strict_types=1);

namespace FlashQ\Tests;

use PHPUnit\Framework\TestCase;
use FlashQ\FlashQ;
use FlashQ\PushOptions;
use FlashQ\CronOptions;
use FlashQ\JobState;
use FlashQ\FlashQException;

/**
 * FlashQ PHP SDK Tests
 *
 * Requirements:
 *   - FlashQ server running on localhost:6789
 *   - Run with: vendor/bin/phpunit tests/
 */
class FlashQTest extends TestCase
{
    private FlashQ $client;

    protected function setUp(): void
    {
        $this->client = new FlashQ();
        $this->client->connect();
    }

    protected function tearDown(): void
    {
        $this->client->close();
    }

    // ============== Connection Tests ==============

    public function testConnect(): void
    {
        $client = new FlashQ();
        $client->connect();
        $this->assertTrue($client->isConnected());
        $client->close();
    }

    // ============== Core Operations ==============

    public function testPush(): void
    {
        $job = $this->client->push('test-queue', ['message' => 'hello']);
        $this->assertGreaterThan(0, $job->id);
        $this->assertEquals('test-queue', $job->queue);

        // Cleanup
        $pulled = $this->client->pull('test-queue');
        $this->client->ack($pulled->id);
    }

    public function testPushWithOptions(): void
    {
        $job = $this->client->push('test-queue', ['task' => 'process'], new PushOptions(
            priority: 10,
            maxAttempts: 3,
            tags: ['important']
        ));
        $this->assertGreaterThan(0, $job->id);
        $this->assertEquals(10, $job->priority);

        // Cleanup
        $pulled = $this->client->pull('test-queue');
        $this->client->ack($pulled->id);
    }

    public function testPushBatch(): void
    {
        $ids = $this->client->pushBatch('batch-queue', [
            ['data' => ['n' => 1]],
            ['data' => ['n' => 2]],
            ['data' => ['n' => 3]],
        ]);
        $this->assertCount(3, $ids);

        // Cleanup
        $jobs = $this->client->pullBatch('batch-queue', 3);
        $this->client->ackBatch(array_map(fn($j) => $j->id, $jobs));
    }

    public function testPullBatch(): void
    {
        // Push first
        $this->client->pushBatch('pullb-queue', [
            ['data' => ['n' => 1]],
            ['data' => ['n' => 2]],
        ]);

        $jobs = $this->client->pullBatch('pullb-queue', 2);
        $this->assertCount(2, $jobs);

        // Cleanup
        $this->client->ackBatch(array_map(fn($j) => $j->id, $jobs));
    }

    public function testPullAndAck(): void
    {
        $job = $this->client->push('pull-test', ['value' => 42]);
        $pulled = $this->client->pull('pull-test');
        $this->assertEquals(42, $pulled->data['value']);
        $this->client->ack($pulled->id, ['processed' => true]);
    }

    public function testFail(): void
    {
        $job = $this->client->push('fail-test', ['will' => 'fail'], new PushOptions(maxAttempts: 2));
        $pulled = $this->client->pull('fail-test');
        $this->client->fail($pulled->id, 'Test failure');

        // Job should be retried or in DLQ
        $state = $this->client->getState($pulled->id);
        $this->assertNotNull($state);
    }

    public function testAckBatch(): void
    {
        $ids = $this->client->pushBatch('ackb-queue', [
            ['data' => ['n' => 1]],
            ['data' => ['n' => 2]],
        ]);
        $jobs = $this->client->pullBatch('ackb-queue', 2);
        $count = $this->client->ackBatch(array_map(fn($j) => $j->id, $jobs));
        $this->assertGreaterThanOrEqual(0, $count);
    }

    // ============== Job Management ==============

    public function testProgress(): void
    {
        $job = $this->client->push('progress-test', ['task' => 'long']);
        $pulled = $this->client->pull('progress-test');
        $this->client->progress($pulled->id, 50, 'Halfway there');

        $prog = $this->client->getProgress($pulled->id);
        $this->assertEquals(50, $prog->progress);
        $this->assertEquals('Halfway there', $prog->message);

        $this->client->ack($pulled->id);
    }

    public function testCancel(): void
    {
        $job = $this->client->push('cancel-test', ['will' => 'cancel']);
        $this->client->cancel($job->id);
        // Verify job was cancelled - state should be unknown (job removed) or cancelled
        $state = $this->client->getState($job->id);
        $this->assertContains($state, [JobState::UNKNOWN, JobState::CANCELLED]);
    }

    public function testGetState(): void
    {
        $job = $this->client->push('state-test', ['data' => 1]);
        $state = $this->client->getState($job->id);
        $this->assertContains($state, [JobState::WAITING, JobState::DELAYED]);

        // Cleanup
        $pulled = $this->client->pull('state-test');
        $this->client->ack($pulled->id);
    }

    public function testGetResult(): void
    {
        $job = $this->client->push('result-test', ['data' => 1]);
        $pulled = $this->client->pull('result-test');
        $this->client->ack($pulled->id, ['answer' => 42]);

        $result = $this->client->getResult($pulled->id);
        $this->assertEquals(['answer' => 42], $result);
    }

    // ============== Queue Control ==============

    public function testPauseResume(): void
    {
        $this->client->pause('pause-test');
        $this->client->resume('pause-test');
        // Should not throw
        $this->assertTrue(true);
    }

    public function testRateLimit(): void
    {
        $this->client->setRateLimit('rate-test', 100);
        $this->client->clearRateLimit('rate-test');
        $this->assertTrue(true);
    }

    public function testConcurrency(): void
    {
        $this->client->setConcurrency('conc-test', 5);
        $this->client->clearConcurrency('conc-test');
        $this->assertTrue(true);
    }

    public function testListQueues(): void
    {
        // Create a queue first
        $this->client->push('list-test', ['n' => 1]);

        $queues = $this->client->listQueues();
        $this->assertIsArray($queues);
        $this->assertGreaterThan(0, count($queues));

        // Cleanup
        $pulled = $this->client->pull('list-test');
        $this->client->ack($pulled->id);
    }

    // ============== DLQ ==============

    public function testDlq(): void
    {
        $job = $this->client->push('dlq-test', ['fail' => true], new PushOptions(maxAttempts: 1));
        $pulled = $this->client->pull('dlq-test');
        $this->client->fail($pulled->id, 'Intentional failure');

        $dlqJobs = $this->client->getDlq('dlq-test');
        $this->assertIsArray($dlqJobs);
    }

    public function testRetryDlq(): void
    {
        $count = $this->client->retryDlq('dlq-test');
        $this->assertGreaterThanOrEqual(0, $count);

        // Cleanup
        while (true) {
            try {
                $job = $this->client->pull('dlq-test');
                $this->client->ack($job->id);
            } catch (\Exception) {
                break;
            }
        }
    }

    // ============== Cron ==============

    public function testAddCron(): void
    {
        $this->client->addCron('test-cron', new CronOptions(
            queue: 'cron-queue',
            data: ['scheduled' => true],
            schedule: '0 0 * * * *',
            priority: 5
        ));
        $this->assertTrue(true);
    }

    public function testListCrons(): void
    {
        $crons = $this->client->listCrons();
        $this->assertIsArray($crons);
        $found = false;
        foreach ($crons as $cron) {
            if ($cron->name === 'test-cron') {
                $found = true;
                break;
            }
        }
        $this->assertTrue($found, 'test-cron not found in cron list');
    }

    public function testDeleteCron(): void
    {
        $result = $this->client->deleteCron('test-cron');
        $this->assertTrue($result);
    }

    // ============== Stats & Metrics ==============

    public function testStats(): void
    {
        $stats = $this->client->stats();
        $this->assertGreaterThanOrEqual(0, $stats->queued);
        $this->assertGreaterThanOrEqual(0, $stats->processing);
        $this->assertGreaterThanOrEqual(0, $stats->delayed);
        $this->assertGreaterThanOrEqual(0, $stats->dlq);
    }

    public function testMetrics(): void
    {
        $metrics = $this->client->metrics();
        $this->assertGreaterThanOrEqual(0, $metrics->totalPushed);
        $this->assertGreaterThanOrEqual(0, $metrics->totalCompleted);
        $this->assertGreaterThanOrEqual(0, $metrics->jobsPerSecond);
    }

    // ============== Advanced ==============

    public function testDependencies(): void
    {
        $parent = $this->client->push('deps-queue', ['type' => 'parent']);
        $child = $this->client->push('deps-queue', ['type' => 'child'], new PushOptions(
            dependsOn: [$parent->id]
        ));

        $state = $this->client->getState($child->id);
        $this->assertContains($state, [JobState::WAITING, JobState::WAITING_CHILDREN]);

        // Cleanup
        $pulled = $this->client->pull('deps-queue');
        $this->client->ack($pulled->id);
    }

    public function testUniqueKey(): void
    {
        $key = 'unique-' . time();
        $job1 = $this->client->push('unique-queue', ['n' => 1], new PushOptions(uniqueKey: $key));

        try {
            $job2 = $this->client->push('unique-queue', ['n' => 2], new PushOptions(uniqueKey: $key));
            $this->assertEquals($job1->id, $job2->id);
        } catch (FlashQException $e) {
            $this->assertStringContainsString('Duplicate', $e->getMessage());
        }

        // Cleanup
        $pulled = $this->client->pull('unique-queue');
        $this->client->ack($pulled->id);
    }
}
