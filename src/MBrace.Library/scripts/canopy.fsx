
#I "../../../lib/"
#r "MBrace.Core.dll"
#r "MBrace.SampleRuntime.exe"
#r "../bin/Debug/MBrace.Library.dll"

open MBrace
open MBrace.Library.Clustering

open System
open MBrace.SampleRuntime

module SyntheticData =
    let create (dimension : int, numberOfPoints : int, partitions) =
        cloud {
            let partitionSize = numberOfPoints / partitions
            let rnd = new Random(DateTime.Now.Millisecond)
            let ps = new ResizeArray<_>()
            for p in [1..partitions] do
                let xs = Seq.init partitionSize (fun _ -> 
                    Array.init dimension (fun _ -> rnd.NextDouble() * 10.0 - 5.0))
                let! cs = CloudSequence.New xs
                ps.Add(cs)
            return ps.ToArray()
        } |> Cloud.ToLocal


MBraceRuntime.WorkerExecutable <- __SOURCE_DIRECTORY__ + "../../../../lib/MBrace.SampleRuntime.exe"
let runtime = MBraceRuntime.InitLocal(4)

let points = runtime.RunLocal <| SyntheticData.create(100, 1000, 8)


let manh (arr1 : Point) (arr2 : Point) =
    Array.fold2 (fun acc x y -> acc + abs (x-y)) 0.0 arr1 arr2

let eucl (arr1 : Point) (arr2 : Point) = 
    Array.fold2 (fun acc elem1 elem2 -> acc + pown (elem1 - elem2) 2) 0.0 arr1 arr2
    |> sqrt

let result = 
    Canopy.calculateClusters(points, 300.0 , manh, 35.0, eucl)
    |> runtime.Run

