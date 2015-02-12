
#I "../../../lib/"
#r "MBrace.Core.dll"
#r "MBrace.SampleRuntime.exe"
#r "../bin/Debug/MBrace.Library.dll"

open MBrace
open MBrace.Library.Clustering

open System
open System.Text
open System.IO
open System.Threading
open System.Globalization
open MBrace.SampleRuntime


let folder = Path.Combine(Path.GetTempPath(), "kmeansFiles")
let fileno = 12

// The k argument of the kmeans algorithm, and the dimension of points.
let k = 3
let dim = 2

[<AutoOpen>]
module SyntheticData =
    do Thread.CurrentThread.CurrentCulture <- CultureInfo.InvariantCulture
    
    // This function converts a sequence of 'a to a sequence of 'a arrays with dimension dim.
    let dimmed (dim : int) (inseq : seq<'a>) : seq<'a[]> =
        let enum = inseq.GetEnumerator()
        seq {
            let hasNext = ref <| enum.MoveNext()
            while !hasNext do
                yield seq {
                    let i = ref 0
                    while !hasNext && !i < dim do
                        yield enum.Current
                        hasNext := enum.MoveNext()
                        i := !i + 1
                    if !i < dim then
                        for j = !i to dim - 1 do
                            yield Unchecked.defaultof<'a>
                }
                |> Seq.toArray
            enum.Dispose()
        }

    // this function creates files with pseudo random content, in a deterministic way. (12 files =~ 512MB)
    let createFiles intArray folder =
        for i in intArray do
            let r = System.Random(i);

            use tw = new StreamWriter(Path.Combine(folder, string i + ".txt")) 
            [|1..64|]
            |> Seq.map (fun _ ->
                let b = StringBuilder()

                [|1..40000|]
                |> Seq.iter (fun _ -> b.Append(r.NextDouble() * 40.0 - 20.0).Append(" ") |> ignore)
    
                b.ToString()
            )
            |> Seq.iter tw.WriteLine
            tw.Dispose()

    do Directory.CreateDirectory(folder) |> ignore
    do createFiles [|1..fileno|] folder


    let parseFile name =
        File.ReadLines name
        |> Seq.collect (fun line -> line.Trim().Split(' '))
        |> Seq.filter(fun x -> x <> "")
        |> Seq.map (float)
        |> dimmed dim

    let mkSequences =
        [|1..fileno|]
        |> Array.map (fun i ->
            Path.Combine(folder, sprintf "%d.txt" i)
                |> parseFile
                |> CloudSequence.New
        ) |> Cloud.Parallel

MBraceRuntime.WorkerExecutable <- __SOURCE_DIRECTORY__ + "../../../../lib/MBrace.SampleRuntime.exe"
let runtime = MBraceRuntime.InitLocal(8)

let refs = runtime.RunLocal mkSequences

let centroids = 
    cloud { 
        let! xs = refs.[0].ToEnumerable()
        return xs |> Seq.take k |> Seq.toArray
    } |> runtime.RunLocal


let result = runtime.Run(KMeans.calculate(refs, centroids, 0))
