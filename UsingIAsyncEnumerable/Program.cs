var path = Path.Combine(Directory.GetCurrentDirectory(), "..", "..", "..", "Data");

// var pipeline = Directory
//     .EnumerateFiles(path)
//     .ToAsyncEnumerable()
//     .SelectAwait(Steps.ReadFile)
//     .Where(Steps.IsValidFileForProcessing)
//     .Select(Steps.CalculateWordCount)
//     .OrderByDescending(x => x.WordCount)
//     .ForEachAsync(Console.WriteLine);

const int batchSize = 2;

var pipeline = Directory
    .EnumerateFiles(path)
    .ToAsyncEnumerable()
    .Batch<string, FilePayload>(batchSize)
    .ProcessEachAsync(Steps.ReadFile)
    .Where(Steps.IsValidFileForProcessing)
    .Select(Steps.CalculateWordCount)
    .OrderByDescending(x => x.WordCount)
    .ForEachAsync(Console.WriteLine);

await pipeline;

public static class Steps
{
    public async static ValueTask<FilePayload> ReadFile(string file)
    {
        var content = await File.ReadAllTextAsync(file);
        var name = Path.GetFileName(file);

        return new FilePayload(name, content);
    }

    public static bool IsValidFileForProcessing(FilePayload file) =>
        file is { Content.Length: > 0, Name: [.., 't', 'x', 't'] };

    public static WordCountPayload CalculateWordCount(FilePayload payload)
    {
        var words = payload.Content.Split(' ');

        return new(payload.Name, words.Length);
    }
}

public record FilePayload(string Name, string Content);
public record WordCountPayload(string Name, int WordCount);

public record PipelineOperation<T, TResult>
{
    public Func<IList<T>, ValueTask<IEnumerable<TResult>>>? Action1 { get; private set; }
    public Func<IList<T>, IEnumerable<TResult>>? Action2 { get; private set; }
    public Func<T, ValueTask<TResult>>? Action3 { get; private set; }
    public Func<T, TResult>? Action4 { get; private set; }

    public Lazy<IAsyncEnumerable<TResult>>? Result { get; set; }

    public IAsyncEnumerable<TResult> ProcessEachAsync(Func<T, ValueTask<TResult>> action)
    {
        Action3 = action;

        return Result.Value!;
    }

    public IAsyncEnumerable<TResult> ProcessEach(Func<T, TResult> action)
    {
        Action4 = action;

        return Result.Value!;
    }

    public IAsyncEnumerable<TResult> ProcessBatchAsync(Func<IList<T>, ValueTask<IEnumerable<TResult>>> action)
    {
        Action1 = action;

        return Result.Value!;
    }

    public IAsyncEnumerable<TResult> ProcessBatch(Func<IList<T>, IEnumerable<TResult>> action)
    {
        Action2 = action;
        return Result.Value!;
    }
}

public static class PipelineBuilderExtensions
{
    public static PipelineOperation<T, TResult> Batch<T, TResult>(this IAsyncEnumerable<T> pipeline, int batchSize)
    {
        var operation = new PipelineOperation<T, TResult>();

        operation.Result = new(() =>
        {
            IAsyncEnumerable<IEnumerable<TResult>> result;

            if (operation.Action1 is not null)
            {
                result = pipeline.Buffer(batchSize).SelectAwait(operation.Action1);
            }
            else if (operation.Action2 is not null)
            {
                result = pipeline.Buffer(batchSize).Select(operation.Action2);
            }
            else if (operation.Action3 is not null)
            {
                result = pipeline
                    .Buffer(batchSize)
                    .SelectAwait(async x => await ProcessEachAsync(x, operation.Action3));
            }
            else if (operation.Action4 is not null)
            {
                result = pipeline.Buffer(batchSize).Select(x => x.Select(y => operation.Action4(y)));
            }
            else
            {
                throw new ArgumentException("Operation is not defined");
            }

            return result.SelectMany(x => x.ToAsyncEnumerable());
        });

        async static Task<IEnumerable<TResult>> ProcessEachAsync(IList<T> values, Func<T, ValueTask<TResult>> action)
        {
            var tasks = values.Select(action).Select(async x => await x).ToList();

            List<TResult> result = [];

            do
            {
                var completedTask = await Task.WhenAny(tasks);
                result.Add(await completedTask);
                tasks.Remove(completedTask);
            } while (tasks.Count > 0);

            return result;
        }

        return operation;
    }

    public static PipelineOperation<T, T> Batch<T>(this IAsyncEnumerable<T> pipeline, int batchSize)
    {
        return Batch<T, T>(pipeline, batchSize);
    }
}