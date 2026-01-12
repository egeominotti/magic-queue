<?php

declare(strict_types=1);

namespace FlashQ;

/**
 * Job state enumeration
 */
enum JobState: string
{
    case WAITING = 'waiting';
    case DELAYED = 'delayed';
    case ACTIVE = 'active';
    case COMPLETED = 'completed';
    case FAILED = 'failed';
    case WAITING_CHILDREN = 'waiting-children';
    case CANCELLED = 'cancelled';
    case UNKNOWN = 'unknown';

    /**
     * Create from string, handling server variations
     */
    public static function fromString(string $value): self
    {
        // Handle server returning 'waitingchildren' without hyphen
        if ($value === 'waitingchildren') {
            return self::WAITING_CHILDREN;
        }
        return self::from($value);
    }
}
