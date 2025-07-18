The s3_daily_storage metricset of aws module allows you to monitor your AWS S3 buckets. `s3_daily_storage` metricset fetches Cloudwatch daily storage metrics for each S3 bucket from [S3 CloudWatch Daily Storage Metrics for Buckets](https://docs.aws.amazon.com/AmazonS3/latest/dev/cloudwatch-monitoring.html).


## AWS Permissions [_aws_permissions_11]

Some specific AWS permissions are required for IAM user to collect AWS s3_daily_storage metrics.

```
ec2:DescribeRegions
cloudwatch:GetMetricData
cloudwatch:ListMetrics
sts:GetCallerIdentity
iam:ListAccountAliases
```


## Dashboard [_dashboard_12]

The aws s3_daily_storage metricset and s3_request metricset shares one predefined dashboard. For example:

![metricbeat aws s3 overview](images/metricbeat-aws-s3-overview.png)

Note: If only `s3_daily_storage` metricset is enabled or s3 request metrics are not enabled for the specific S3 bucket, some visualizations in this dashboard will be empty.


## Configuration example [_configuration_example_11]

```yaml
- module: aws
  period: 86400s
  metricsets:
    - s3_daily_storage
  access_key_id: '<access_key_id>'
  secret_access_key: '<secret_access_key>'
  session_token: '<session_token>'
```
