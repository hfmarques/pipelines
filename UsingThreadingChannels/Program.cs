using System.Threading.Channels;

//******************************************************************************************************************************
//this is only for learning purposes, don't use it in production. For production an alternative is to use Open.ChannelExtensions
//******************************************************************************************************************************

// var pipeline = Source(Generate(1, 2, 3));

List<long> arr = [0, 0, 0];

async Task func1 (List<long> a1, int index)
{
    a1[index]++;
    Console.SetCursorPosition(0,0);
    Console.Write($"{a1[0]} | {a1[1]} | {a1[2]}");
};

var pipeline = Source(GenerateRange(0..2))
    .CustomPipe(x => (item: x, square: x * x))
    .CustomPipeAsync(
        maxConcurrency: 3,
        async x =>
        {
            // await Task.Delay(x.square * 10);
            await func1(arr, x.item);
            return x;
        }
    )
    .CustomPipe(x => $"{x.item,2}^2 = {x.square,4}");

await pipeline.ForEach(Console.WriteLine);

async static IAsyncEnumerable<T> Generate<T>(params T[] array)
{
    foreach (var item in array)
    {
        yield return item;
    }
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

internal static class ChannelReaderExtensions
{
    private static ChannelReader<T> Merge<T>(params ChannelReader<T>[] inputs)
    {
        var output = Channel.CreateUnbounded<T>();

        Task.Run(async () =>
        {
            async Task Redirect(ChannelReader<T> input)
            {
                await foreach (var item in input.ReadAllAsync())
                    await output.Writer.WriteAsync(item);
            }

            await Task.WhenAll(inputs.Select(i => Redirect(i)).ToArray());
            output.Writer.Complete();
        });

        return output;
    }

    private static ChannelReader<T>[] Split<T>(ChannelReader<T> ch, int n)
    {
        var outputs = Enumerable.Range(0, n)
            .Select(_ => Channel.CreateUnbounded<T>())
            .ToArray();

        Task.Run(async () =>
        {
            var index = 0;
            await foreach (var item in ch.ReadAllAsync())
            {
                await outputs[index].Writer.WriteAsync(item);
                index = (index + 1) % n;
            }

            foreach (var output in outputs)
                output.Writer.Complete();
        });

        return outputs.Select(output => output.Reader).ToArray();
    }
    
    public async static Task ForEach<TRead>(this ChannelReader<TRead> source, Action<TRead> action)
    {
        await foreach (var item in source.ReadAllAsync())
        {
            action(item);
        }
    }
    
    public static ChannelReader<TOut> CustomPipe<TRead, TOut>(
        this ChannelReader<TRead> source,
        Func<TRead, TOut> transform
    )
    {
        var channel = Channel.CreateUnbounded<TOut>();

        Task.Run(async () =>
        {
            await foreach (var item in source.ReadAllAsync())
            {
                await channel.Writer.WriteAsync(transform(item));
            }

            channel.Writer.Complete();
        });

        return channel.Reader;
    }
    
    public static ChannelReader<TOut> CustomPipeAsync<TRead, TOut>(
        this ChannelReader<TRead> source,
        Func<TRead, ValueTask<TOut>> transform
    )
    {
        var channel = Channel.CreateUnbounded<TOut>();

        Task.Run(async () =>
        {
            await foreach (var item in source.ReadAllAsync())
            {
                await channel.Writer.WriteAsync(await transform(item));
            }

            channel.Writer.Complete();
        });

        return channel.Reader;
    }
    
    public static ChannelReader<TOut> CustomPipeAsync<TRead, TOut>(
        this ChannelReader<TRead> source,
        int maxConcurrency,
        Func<TRead, ValueTask<TOut>> transform
    )
    {
        var bufferChannel = Channel.CreateUnbounded<TOut>();

        var channel = Merge(Split(source, maxConcurrency));

        Task.Run(async () =>
        {
            await foreach (var item in channel.ReadAllAsync())
            {
                await bufferChannel.Writer.WriteAsync(await transform(item));
            }

            bufferChannel.Writer.Complete();
        });

        return bufferChannel.Reader;
    }
}
