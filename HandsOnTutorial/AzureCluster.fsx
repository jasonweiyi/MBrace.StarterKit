﻿#I __SOURCE_DIRECTORY__
#I "../packages/MBrace.Azure/tools" 
#I "../packages/Streams/lib/net45" 
#r "../packages/Streams/lib/net45/Streams.dll"
#I "../packages/MBrace.Flow/lib/net45" 
#r "../packages/MBrace.Flow/lib/net45/MBrace.Flow.dll"
#load "../packages/MBrace.Azure/MBrace.Azure.fsx"
#load "../packages/MBrace.Azure.Management/MBrace.Azure.Management.fsx"

namespace global

module Config =

    open MBrace.Core
    open MBrace.Runtime
    open MBrace.Azure
    open MBrace.Azure.Management

    // This script is used to reconnect to your cluster.

    // You can download your publication settings file at 
    //     https://manage.windowsazure.com/publishsettings
    let pubSettingsFile = @"C:\path\to\my.publishsettings" 

    // Your cluster name is reported when you create your cluster, or can be found in 
    // the Azure management console.
    let clusterName = "... enter your cluster name here ..."

    // Connect to the cluster 
    let GetCluster() = 
        let deployment = Deployment.GetDeployment(pubSettingsFile, clusterName)
        AzureCluster.Connect(deployment.Configuration, logger = ConsoleLogger(true), logLevel = LogLevel.Info)



