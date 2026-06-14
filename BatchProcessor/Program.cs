




Console.WriteLine("Large batch processor started");

using var cancellationTokenSource = new CancellationTokenSource();

// Change this if you want to test cancellation.
// For 1 million fake jobs, you may want a longer timeout.
cancellationTokenSource.CancelAfter(TimeSpan.FromSeconds(30));

CancellationToken cancellationToken = cancellationTokenSource.Token;

// For learning, start with 100 or 1,000.
// Then try 1_000_000.
List<FakeJob> jobs = Enumerable.Range(1, 100_000)
    .Select(id => new FakeJob(
        Id: id,
        Name: $"Job {id}",
        Payload: $"Fake payload for job {id}"))
    .ToList();

var processor = new LargeBatchProcessor(
    workerCount: 10,
    maxRetryAttempts: 3);

BatchSummary summary = await processor.ProcessAsync(
    jobs,
    cancellationToken);

Console.WriteLine();
Console.WriteLine("Batch finished");
Console.WriteLine($"Total: {summary.Total}");
Console.WriteLine($"Succeeded: {summary.Succeeded}");
Console.WriteLine($"Failed: {summary.Failed}");
Console.WriteLine($"Cancelled: {summary.Cancelled}");

public sealed class LargeBatchProcessor
{
    private readonly int _workerCount;
    private readonly int _maxRetryAttempts;

    public LargeBatchProcessor(
        int workerCount,
        int maxRetryAttempts)
    {
        if (workerCount <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(workerCount),
                "Worker count must be greater than zero.");
        }

        if (maxRetryAttempts <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxRetryAttempts),
                "Max retry attempts must be greater than zero.");
        }

        _workerCount = workerCount;
        _maxRetryAttempts = maxRetryAttempts;
    }

    public async Task<BatchSummary> ProcessAsync(
        IReadOnlyList<FakeJob> jobs,
        CancellationToken cancellationToken)
    {
        var jobSource = new JobSource(jobs);

        int total = 0;
        int succeeded = 0;
        int failed = 0;
        int cancelled = 0;

        Task[] workers = Enumerable.Range(1, _workerCount)
            .Select(workerId => WorkerAsync(
                workerId,
                jobSource,
                cancellationToken,
                onJobCompleted: result =>
                {
                    Interlocked.Increment(ref total);

                    if (result.Success)
                    {
                        Interlocked.Increment(ref succeeded);
                    }
                    else if (result.Cancelled)
                    {
                        Interlocked.Increment(ref cancelled);
                    }
                    else
                    {
                        Interlocked.Increment(ref failed);
                    }

                    int processedSoFar = Volatile.Read(ref total);

                    if (processedSoFar % 10_000 == 0)
                    {
                        Console.WriteLine(
                            $"Progress: {processedSoFar:N0}/{jobs.Count:N0} jobs processed");
                    }
                }))
            .ToArray();

        await Task.WhenAll(workers);

        return new BatchSummary(
            Total: total,
            Succeeded: succeeded,
            Failed: failed,
            Cancelled: cancelled);
    }

    private async Task WorkerAsync(
        int workerId,
        JobSource jobSource,
        CancellationToken cancellationToken,
        Action<JobResult> onJobCompleted)
    {
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            FakeJob? job = jobSource.GetNextJob();

            if (job is null)
            {
                break;
            }

            JobResult result = await ProcessJobWithRetryAsync(
                workerId,
                job,
                cancellationToken);

            onJobCompleted(result);
        }
    }

    private async Task<JobResult> ProcessJobWithRetryAsync(
        int workerId,
        FakeJob job,
        CancellationToken cancellationToken)
    {
        for (int attempt = 1; attempt <= _maxRetryAttempts; attempt++)
        {
            try
            {
                await DoFakeWorkAsync(workerId, job, cancellationToken);

                return new JobResult(
                    JobId: job.Id,
                    Success: true,
                    Cancelled: false,
                    Attempts: attempt,
                    Message: $"Processed successfully on attempt {attempt}");
            }
            catch (OperationCanceledException)
            {
                return new JobResult(
                    JobId: job.Id,
                    Success: false,
                    Cancelled: true,
                    Attempts: attempt,
                    Message: "Job was cancelled");
            }
            catch (Exception ex)
            {
                if (attempt == _maxRetryAttempts)
                {
                    return new JobResult(
                        JobId: job.Id,
                        Success: false,
                        Cancelled: false,
                        Attempts: attempt,
                        Message: $"Failed after {attempt} attempts. Last error: {ex.Message}");
                }

                await Task.Delay(
                    TimeSpan.FromMilliseconds(500),
                    cancellationToken);
            }
        }

        return new JobResult(
            JobId: job.Id,
            Success: false,
            Cancelled: false,
            Attempts: _maxRetryAttempts,
            Message: "Unexpected failure");
    }

    private static async Task DoFakeWorkAsync(
        int workerId,
        FakeJob job,
        CancellationToken cancellationToken)
    {
        // Print only occasionally, otherwise 1 million console logs will be very slow.
        if (job.Id <= 20 || job.Id % 50_000 == 0)
        {
            Console.WriteLine(
                $"Worker {workerId} processing {job.Name}");
        }

        int delay = Random.Shared.Next(5, 30);

        await Task.Delay(delay, cancellationToken);

        bool shouldFail = Random.Shared.NextDouble() < 0.01;

        if (shouldFail)
        {
            throw new InvalidOperationException(
                $"Could not process payload: {job.Payload}");
        }
    }
}

public sealed class JobSource
{
    private readonly IReadOnlyList<FakeJob> _jobs;
    private readonly object _lockObject = new();

    private int _nextJobIndex;

    public JobSource(IReadOnlyList<FakeJob> jobs)
    {
        _jobs = jobs;
    }

    public FakeJob? GetNextJob()
    {
        lock (_lockObject)
        {
            if (_nextJobIndex >= _jobs.Count)
            {
                return null;
            }

            FakeJob job = _jobs[_nextJobIndex];

            _nextJobIndex++;

            return job;
        }
    }
}

public sealed record FakeJob(
    int Id,
    string Name,
    string Payload
);

public sealed record JobResult(
    int JobId,
    bool Success,
    bool Cancelled,
    int Attempts,
    string Message
);

public sealed record BatchSummary(
    int Total,
    int Succeeded,
    int Failed,
    int Cancelled
);


//// Batch processor example(Single task)
//Console.WriteLine("Batch processor started");

//using var cancellationTokenSource = new CancellationTokenSource();

//cancellationTokenSource.CancelAfter(TimeSpan.FromSeconds(10));

//CancellationToken cancellationToken = cancellationTokenSource.Token;

//List<FakeJob> jobs = Enumerable.Range(1, 100)
//    .Select(id => new FakeJob(
//        Id: id,
//        Name: $"Job {id}",
//        Payload: $"Fake payload for job {id}"))
//    .ToList();

//var processor = new BatchProcessor(
//    maxConcurrency: 10,
//    maxRetryAttempts: 3);

//JobResult[] results = await processor.ProcessAsync(jobs, cancellationToken);

//PrintSummary(results);

//Console.WriteLine("Batch processor finished");

//static void PrintSummary(JobResult[] results)
//{
//    Console.WriteLine();
//    Console.WriteLine("Batch results:");
//    Console.WriteLine($"Total jobs: {results.Length}");
//    Console.WriteLine($"Succeeded: {results.Count(r => r.Success)}");
//    Console.WriteLine($"Failed: {results.Count(r => !r.Success && !r.Cancelled)}");
//    Console.WriteLine($"Cancelled: {results.Count(r => r.Cancelled)}");

//    Console.WriteLine();
//    Console.WriteLine("First 10 results:");

//    foreach (JobResult result in results.Take(10))
//    {
//        Console.WriteLine(
//            $"{result.JobName}: " +
//            $"{GetStatusText(result)} - " +
//            result.Message);
//    }
//}

//static string GetStatusText(JobResult result)
//{
//    if (result.Success)
//    {
//        return "Success";
//    }

//    if (result.Cancelled)
//    {
//        return "Cancelled";
//    }

//    return "Failed";
//}

//public sealed class BatchProcessor
//{
//    private readonly int _maxConcurrency;
//    private readonly int _maxRetryAttempts;

//    public BatchProcessor(int maxConcurrency, int maxRetryAttempts)
//    {
//        if (maxConcurrency <= 0)
//        {
//            throw new ArgumentOutOfRangeException(
//                nameof(maxConcurrency),
//                "Max concurrency must be greater than zero.");
//        }

//        if (maxRetryAttempts <= 0)
//        {
//            throw new ArgumentOutOfRangeException(
//                nameof(maxRetryAttempts),
//                "Max retry attempts must be greater than zero.");
//        }

//        _maxConcurrency = maxConcurrency;
//        _maxRetryAttempts = maxRetryAttempts;
//    }

//    public async Task<JobResult[]> ProcessAsync(
//        IReadOnlyList<FakeJob> jobs,
//        CancellationToken cancellationToken)
//    {
//        using var semaphore = new SemaphoreSlim(_maxConcurrency);

//        int completedCount = 0;
//        int totalCount = jobs.Count;

//        Task<JobResult>[] tasks = jobs
//            .Select(async job =>
//            {
//                JobResult result = await ProcessJobWithLimitAsync(
//                    job,
//                    semaphore,
//                    cancellationToken);

//                int finished = Interlocked.Increment(ref completedCount);

//                Console.WriteLine(
//                    $"Progress: {finished}/{totalCount} completed");

//                return result;
//            })
//            .ToArray();

//        JobResult[] results = await Task.WhenAll(tasks);

//        return results;
//    }

//    private async Task<JobResult> ProcessJobWithLimitAsync(
//        FakeJob job,
//        SemaphoreSlim semaphore,
//        CancellationToken cancellationToken)
//    {
//        try
//        {
//            await semaphore.WaitAsync(cancellationToken);

//            try
//            {
//                return await ProcessJobWithRetryAsync(job, cancellationToken);
//            }
//            finally
//            {
//                semaphore.Release();
//            }
//        }
//        catch (OperationCanceledException)
//        {
//            return new JobResult(
//                JobId: job.Id,
//                JobName: job.Name,
//                Success: false,
//                Cancelled: true,
//                Attempts: 0,
//                Message: "Job was cancelled");
//        }
//    }

//    private async Task<JobResult> ProcessJobWithRetryAsync(
//        FakeJob job,
//        CancellationToken cancellationToken)
//    {
//        for (int attempt = 1; attempt <= _maxRetryAttempts; attempt++)
//        {
//            try
//            {
//                Console.WriteLine($"{job.Name} attempt {attempt} started");

//                await DoFakeWorkAsync(job, cancellationToken);

//                return new JobResult(
//                    JobId: job.Id,
//                    JobName: job.Name,
//                    Success: true,
//                    Cancelled: false,
//                    Attempts: attempt,
//                    Message: $"Processed '{job.Payload}' on attempt {attempt}");
//            }
//            catch (OperationCanceledException)
//            {
//                return new JobResult(
//                    JobId: job.Id,
//                    JobName: job.Name,
//                    Success: false,
//                    Cancelled: true,
//                    Attempts: attempt,
//                    Message: "Job was cancelled");
//            }
//            catch (Exception ex)
//            {
//                if (attempt == _maxRetryAttempts)
//                {
//                    return new JobResult(
//                        JobId: job.Id,
//                        JobName: job.Name,
//                        Success: false,
//                        Cancelled: false,
//                        Attempts: attempt,
//                        Message: $"Failed after {attempt} attempts. Last error: {ex.Message}");
//                }

//                Console.WriteLine(
//                    $"{job.Name} failed on attempt {attempt}. Retrying...");

//                await Task.Delay(500, cancellationToken);
//            }
//        }

//        return new JobResult(
//            JobId: job.Id,
//            JobName: job.Name,
//            Success: false,
//            Cancelled: false,
//            Attempts: _maxRetryAttempts,
//            Message: "Job failed unexpectedly");
//    }

//    private static async Task DoFakeWorkAsync(
//        FakeJob job,
//        CancellationToken cancellationToken)
//    {
//        int delay = Random.Shared.Next(500, 3000);

//        await Task.Delay(delay, cancellationToken);

//        bool shouldFail = Random.Shared.NextDouble() < 0.30;

//        if (shouldFail)
//        {
//            throw new InvalidOperationException(
//                $"Could not process payload: {job.Payload}");
//        }
//    }
//}

//public sealed record FakeJob(
//    int Id,
//    string Name,
//    string Payload
//);

//public sealed record JobResult(
//    int JobId,
//    string JobName,
//    bool Success,
//    bool Cancelled,
//    int Attempts,
//    string Message
//);