Q1:

![](https://cdn.nlark.com/yuque/0/2025/png/2675852/1737955447180-f2bf7f60-23c3-43b0-aa86-974d5be03552.png)



Q2:

The service name of postgres database is "db", and the port is 5432



Q3:

The SQL code is as follows:

```sql
select
	count(case when trip_distance <= 1 then 1 end) as first_part,
	count(case when trip_distance > 1 and trip_distance <=3 then 1 end) as second_part,
	count(case when trip_distance > 3 and trip_distance <=7 then 1 end) as third_part,
	count(case when trip_distance > 7 and trip_distance <=10 then 1 end) as fourth_part,
	count(case when trip_distance > 10 then 1 end) as fifth_part
from green_taxi_trips
where
CAST(lpep_pickup_datetime AS DATE) >= '2019-10-01'
AND
CAST(lpep_dropoff_datetime AS DATE) < '2019-11-01';
```



Q4:

The SQL code is as follows:

```sql
select 
	cast(lpep_pickup_datetime as date) as date_,
	max(trip_distance) as max_trip_distance
from green_taxi_trips 
group by cast(lpep_pickup_datetime as date) 
order by max_trip_distance desc
limit 1
```



Q5:

The SQL code is as follows:

```sql
select 
	zpu."Zone"
from 
	green_taxi_trips gtt
join
	zones zpu
on gtt."PULocationID" = zpu."LocationID"
where 
cast(lpep_pickup_datetime as date) = '2019-10-18' 
group by zpu."Zone"
having sum(total_amount) > 13000
```



Q6:

The SQL code is as follows:

```sql
select 
	tip_amount,
	zdo."Zone"
from 
	green_taxi_trips gtt
join
	zones zpu
on gtt."PULocationID" = zpu."LocationID"
join 
	zones zdo
on gtt."DOLocationID" = zdo."LocationID"
where zpu."Zone" ='East Harlem North'
order by tip_amount desc
```



Q7:

1. `terraform init`: initializes the Terraform working directory and downloads the necessary provider plugins
2. `terraform apply`: used to apply the changes, and we can use the `-auto-approve` flag to skip the manual approval step, which means it directly executes the plan
3. `terraform destroy`: removes all resources defined in your configuration

