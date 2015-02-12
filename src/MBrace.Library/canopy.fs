namespace MBrace.Library.Clustering

#nowarn "0444"

[<RequireQualifiedAccessAttribute>]
module Canopy =
    open MBrace
    open System.Collections.Generic

    let private canopyLocal 
                    (points : CloudSequence<Point>) 
                    (canopy : Point) 
                    (t1 : float) 
                    (distance1 : Point -> Point -> float)
                    (t2 : float) 
                    (distance2 : Point -> Point -> float) =
        cloud {
            let! pts = points.ToEnumerable()
            let pointsToRemove = ref Set.empty
            let pointsBelongToCanopy = new ResizeArray<Point>()
            for pt in pts do
                let d1 = distance1 pt canopy
                if d1 < t1 then
                    pointsBelongToCanopy.Add(pt)
                    let d2 = distance2 pt canopy
                    if d2 < t2 then
                        pointsToRemove := Set.add pt pointsToRemove.Value
            let! remove = CloudRef.New pointsToRemove.Value
            return remove, pointsBelongToCanopy
        }

    let private removePoints (points : CloudSequence<Point>) (pointsToRemove : CloudRef<Set<Point>>) : Cloud<CloudSequence<Point>> =
        cloud {
            let! ps = points.ToEnumerable()
            let! remove = pointsToRemove.Value
            let newPoints =
                ps |> Seq.filter (fun pt -> not <| remove.Contains(pt))
            return! CloudSequence.New newPoints
        }

    //
    // http://en.wikipedia.org/wiki/Canopy_clustering_algorithm
    //
    // 1. Begin with the set of data points to be clustered.
    // 2. Remove a point from the set, beginning a new 'canopy'.
    // 3. For each point left in the set, assign it to the new canopy if the distance less than the loose distance T_1.
    // 4. If the distance of the point is additionally less than the tight distance T_2, remove it from the original set.
    // 5. Repeat from step 2 until there are no more data points in the set to cluster.
    // 6. These relatively cheaply clustered canopies can be sub-clustered using a more expensive but accurate algorithm.

    let calculateClusters(points : CloudSequence<Point> [], t1 : float, distancef1, t2 : float, distancef2) =
        let rec loop (points : CloudSequence<Point> []) t1 distancef1 t2 distancef2 (canopies : Map<Point, CloudSequence<Point>> ) =
            cloud {
                let! canopy = cloud {
                    if Array.isEmpty points then 
                        return None
                    else
                        let! xs = (Seq.head points).ToEnumerable()
                        return Some(Seq.head xs)
                }

                match canopy with
                | None -> return canopies
                | Some canopy ->
                    // calculate points that belong to canopy and points that need to be removed from 'points'.
                    let! partial = 
                        points 
                        |> Seq.map (fun pts -> canopyLocal pts canopy t1 distancef1 t2 distancef2)
                        |> Cloud.Parallel 

                    // points that belong to canopy center.
                    let! values =
                        partial
                        |> Seq.map snd
                        |> Seq.concat
                        |> CloudSequence.New
                    let canopies = canopies.Add(canopy, values)

                    // remove points
                    let! xs = 
                        partial
                        |> Seq.map fst
                        |> Seq.map2 (fun ps xs -> removePoints ps xs) points
                        |> Cloud.Parallel
                    // keep only non-empty sequences
                    let! newPoints =
                        xs
                        |> Workflows.Sequential.filter (fun cs -> 
                            cloud { let! l = cs.Count in return l > 0 })

                    return! loop newPoints t1 distancef1 t2 distancef2 canopies
            }

        loop points t1 distancef1 t2 distancef2 Map.empty