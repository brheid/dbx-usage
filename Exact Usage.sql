-- Databricks notebook source

Create or replace temporary view v_exactDbus AS
Select date, yyyymm, monthName, cloudType, workloadType, sfdcAccountName, workspaceId, workspaceName, workspaceStatus, shardName, shardType, nodeTypeName, clusterId, deltaDbus, nonDeltaDbus, dbus, listPrice
From prod_ds.workloads_cluster_agg
Where sfdcAccountId = "00161000011Yj88AAC" and Date > date('2021-01-01')

-- COMMAND ----------


Select * From v_exactDbus Limit 10

-- COMMAND ----------


Select sfdcAccountName, cloudType, listPrice, Sum(deltaDbus), sum(nonDeltaDbus), sum(dbus), sum(dbus*listPrice) as dollarDbus
From v_exactDbus
--Where monthName = "JUNE"
Where datediff(now(),date)<30
Group by sfdcAccountName, cloudType, listPrice
Order by cloudType
