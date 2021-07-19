-- Databricks notebook source

Create or replace temporary view v_thriveDbus AS
Select date, yyyymm, monthName, cloudType, workloadType, sfdcAccountName, workspaceId, workspaceName, workspaceStatus, shardName, shardType, nodeTypeName, clusterId, deltaDbus, nonDeltaDbus, dbus, listPrice
From prod_ds.workloads_cluster_agg
Where sfdcAccountId = "0014N00001g8CvNQAU" and Date > date('2021-01-01')

-- COMMAND ----------


Select * From v_thriveDbus Limit 10

-- COMMAND ----------


Select sfdcAccountName, cloudType, listPrice, Sum(deltaDbus), sum(nonDeltaDbus), sum(dbus), sum(dbus*listPrice) as dollarDbus
From v_thriveDbus
--Where monthName = "JUNE"
Where datediff(now(),date)<30
Group by sfdcAccountName, cloudType, listPrice
Order by cloudType
