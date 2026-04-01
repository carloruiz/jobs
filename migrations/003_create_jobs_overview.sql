-- Compatible with PostgreSQL 12+ and CockroachDB 22.1+.
-- LEFT JOIN LATERAL is supported in CockroachDB since v21.1 and in
-- PostgreSQL since v9.3. CASE expressions and views are fully supported
-- by both engines.
--
-- jobs_overview is the canonical way to observe job state. It joins jobs with
-- the most recent attempt, including jobs that have never been attempted.
-- Use this view for debugging and ad-hoc investigation; do not use it in
-- claim logic.
CREATE VIEW jobs_overview AS
SELECT
    j.id           AS job_id,
    j.name,
    j.namespace,
    j.max_attempts,
    j.retry_until,
    j.deadline,
    j.created_at,
    a.attempt_no,
    j.request,
    a.response,
    a.error,
    a.started_at,
    a.finished_at,
    a.executor_host,
    CASE
        WHEN a.attempt_no IS NULL                   THEN 'pending'
        WHEN a.finished_at IS NULL                  THEN 'running'
        WHEN a.error IS NOT NULL
             AND a.attempt_no >= j.max_attempts
             AND (j.retry_until IS NULL OR now() >= j.retry_until)
                                                    THEN 'failed'
        WHEN a.error IS NOT NULL                    THEN 'pending_retry'
        ELSE                                             'complete'
    END            AS status
FROM jobs j
LEFT JOIN LATERAL (
    SELECT * FROM job_attempts
    WHERE job_id = j.id
    ORDER BY attempt_no DESC
    LIMIT 1
) a ON true;
