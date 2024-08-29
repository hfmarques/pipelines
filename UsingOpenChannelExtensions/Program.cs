using System.Threading.Channels;
using Open.ChannelExtensions;

var pipeline = Source(GenerateRange(1..10))
    .Pipe(x => (item: x, square: x * x))
    .PipeAsync(
        maxConcurrency: 2,
        async x =>
        {
            await Task.Delay(x.square * 10);

            return x;
        }
    )
    .Pipe(x => $"{x.item,2}^2 = {x.square,4}");

await foreach (var item in pipeline.ReadAllAsync())
{
    Console.WriteLine(item);
}

async static IAsyncEnumerable<int> GenerateRange(Range range)
{
    var count = range.End.Value - range.Start.Value + 1;
    foreach (var item in Enumerable.Range(range.Start.Value, count))
    {
        yield return item;
    }
}

static ChannelReader<TOut> Source<TOut>(IAsyncEnumerable<TOut> source)
{
    var channel = Channel.CreateUnbounded<TOut>();

    Task.Run(async () =>
    {
        await foreach (var item in source)
        {
            await channel.Writer.WriteAsync(item);
        }

        channel.Writer.Complete();
    });

    return channel.Reader;
}