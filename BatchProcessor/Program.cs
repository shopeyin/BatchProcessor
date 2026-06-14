




using System.Collections.Concurrent;

Console.WriteLine("Large batch processor started");

using var cancellationTokenSource = new CancellationTokenSource();

// Increase, reduce, or comment this out to test cancellation.
// cancellationTokenSource.CancelAfter(TimeSpan.FromSeconds(30));

CancellationToken cancellationToken = cancellationTokenSource.Token;

List<FakeJob> jobs = Enumerable.Range(1, 1000)
    .Select(id => new FakeJob(
        Id: id,
        Name: $"Job {id}",
        Payload: $"Fake payload for job {id}"))
    .ToList();

var processor = new LargeBatchProcessor(
    workerCount: 10,
    maxRetryAttempts: 3);

BatchResult batchResult = await processor.ProcessAsync(
    jobs,
    cancellationToken);

Console.WriteLine();
Console.WriteLine("Batch finished");
Console.WriteLine($"Total: {batchResult.Summary.Total}");
Console.WriteLine($"Succeeded: {batchResult.Summary.Succeeded}");
Console.WriteLine($"Failed: {batchResult.Summary.Failed}");
Console.WriteLine($"Cancelled: {batchResult.Summary.Cancelled}");

Console.WriteLine();
Console.WriteLine("First 10 stored results:");

foreach (JobResult result in batchResult.Results.Values
             .OrderBy(r => r.JobId)
             .Take(10))
{
    Console.WriteLine(
        $"Job {result.JobId}: {GetStatusText(result)} - {result.Message}");
}

static string GetStatusText(JobResult result)
{
    if (result.Success)
    {
        return "Success";
    }

    if (result.Cancelled)
    {
        return "Cancelled";
    }

    return "Failed";
}

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

    public async Task<BatchResult> ProcessAsync(
        IReadOnlyList<FakeJob> jobs,
        CancellationToken cancellationToken)
    {
        var queue = new ConcurrentQueue<FakeJob>(jobs);

        var results = new ConcurrentDictionary<int, JobResult>();

        int total = 0;
        int succeeded = 0;
        int failed = 0;
        int cancelled = 0;

        Task[] workers = Enumerable.Range(1, _workerCount)
            .Select(workerId => WorkerAsync(
                workerId,
                queue,
                results,
                jobs.Count,
                cancellationToken,
                result =>
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

        var summary = new BatchSummary(
            Total: total,
            Succeeded: succeeded,
            Failed: failed,
            Cancelled: cancelled);

        return new BatchResult(
            Summary: summary,
            Results: results);
    }

    private async Task WorkerAsync(
        int workerId,
        ConcurrentQueue<FakeJob> queue,
        ConcurrentDictionary<int, JobResult> results,
        int totalJobCount,
        CancellationToken cancellationToken,
        Action<JobResult> onJobCompleted)
    {
        while (!cancellationToken.IsCancellationRequested &&
               queue.TryDequeue(out FakeJob? job))
        {
            JobResult result = await ProcessJobWithRetryAsync(
                workerId,
                job,
                cancellationToken);

            results[result.JobId] = result;

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
                await DoFakeWorkAsync(
                    workerId,
                    job,
                    cancellationToken);

                return new JobResult(
                    JobId: job.Id,
                    JobName: job.Name,
                    Success: true,
                    Cancelled: false,
                    Attempts: attempt,
                    Message: $"Processed successfully on attempt {attempt}");
            }
            catch (OperationCanceledException)
            {
                return new JobResult(
                    JobId: job.Id,
                    JobName: job.Name,
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
                        JobName: job.Name,
                        Success: false,
                        Cancelled: false,
                        Attempts: attempt,
                        Message: $"Failed after {attempt} attempts. Last error: {ex.Message}");
                }

                Console.WriteLine(
                    $"{job.Name} failed on attempt {attempt}. Retrying...");

                try
                {
                    await Task.Delay(
                        TimeSpan.FromMilliseconds(500),
                        cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    return new JobResult(
                        JobId: job.Id,
                        JobName: job.Name,
                        Success: false,
                        Cancelled: true,
                        Attempts: attempt,
                        Message: "Job was cancelled while waiting to retry");
                }
            }
        }

        return new JobResult(
            JobId: job.Id,
            JobName: job.Name,
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

public sealed record FakeJob(
    int Id,
    string Name,
    string Payload
);

public sealed record JobResult(
    int JobId,
    string JobName,
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

public sealed record BatchResult(
    BatchSummary Summary,
    ConcurrentDictionary<int, JobResult> Results
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