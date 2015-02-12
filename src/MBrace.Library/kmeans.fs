namespace MBrace.Library.Clustering

open MBrace
open System

/// Represents a multi-dimensional point.
type Point = float[]

[<RequireQualifiedAccess>]
/// K-Means algorithm implementation.
module KMeans =

    // This function partitions an array into n arrays.
    let internal partition n (a : _ array) =
        [| for i in 0 .. n - 1 ->
            let i, j = a.Length * i / n, a.Length * (i + 1) / n
            Array.sub a i (j - i) |]

    // This function calculates the distance between two points.
    let internal dist (arr1 : Point) (arr2 : Point) = Array.fold2 (fun acc elem1 elem2 -> acc + pown (elem1 - elem2) 2) 0.0 arr1 arr2

    // This function assigns a point to the correct centroid, and returns the index of that centroid.
    let private findCentroid (p : Point) (centroids : Point[]) : int =
        let mutable mini = 0
        let mutable min = Double.MaxValue
        for i in 0..(centroids.Length - 1) do
            let dist = dist p centroids.[i]
            if dist < min then
                min <- dist
                mini <- i

        mini

    /// This function, given a portion of the points, calculates the number of the points assigned to each centroid,
    /// as well as their sum.
    let private kmeansLocal (points : Point seq) (centroids : Point[]) : (int * (int * Point))[] =
        let lens = Array.init centroids.Length (fun _ -> 0)
        let sums : Point[] = Array.init centroids.Length (fun _ -> Array.init centroids.[0].Length (fun _ -> 0.0 ))
        for point in points do
            let cent = findCentroid point centroids
            lens.[cent] <- lens.[cent] + 1
            for i in 0..(point.Length - 1) do
                sums.[cent].[i] <- sums.[cent].[i] + point.[i]

        Array.init centroids.Length (fun i -> (i, (lens.[i], sums.[i])))

    /// The function runs the kmeans algorithm and returns the centroids of an array of Cloud Sequences of points.
    let rec calculate (points : CloudSequence<Point>[], centroids : Point[], iteration : int) : Cloud<Point[]> =
        let sumPoints (pointArr : seq<Point>) dim : Point =
            pointArr
            |> Seq.fold (fun acc elem -> let x = Array.map2 (+) acc elem in x) (Array.init dim (fun _ -> 0.0))

        let divPoint (point : Point) (x : float) : Point =
            Array.map (fun p -> p / x) point

        cloud {
            do! Cloud.Logf "KMeans iteration [%d] with centroids \n %A" iteration centroids
            let dim = centroids.[0].Length

            let! clusterParts =
                points
                |> Array.map (fun part -> cloud { let! points = part.ToEnumerable() in return kmeansLocal points centroids })
                |> Cloud.Parallel

            let newCent =
                clusterParts
                |> Seq.concat
                |> Seq.groupBy fst
                |> Seq.map snd
                |> Seq.map (fun clp ->
                    clp
                    |> Seq.fold (fun (accN, accSum) (_, (n, sum)) -> (accN + n, sumPoints [|accSum; sum|] dim)) (0, Array.init dim (fun _ -> 0.0))
                )
                |> Seq.map (fun (n, sum) -> divPoint sum (float n))
                |> Seq.toArray

            if Array.forall2 (Array.forall2 (fun x y -> abs(x - y) < 1E-10)) newCent centroids then
                return centroids
            else
                return! calculate(points, newCent, (iteration + 1))
        }
