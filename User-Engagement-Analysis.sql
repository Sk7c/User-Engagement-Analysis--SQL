--create table in postgreSQL
create table public.analysis (
session_id INT 
,customer_id INT 
,login_date DATE
,projects_added BOOLEAN
,likes_given BOOLEAN
,comment_given BOOLEAN
,inactive_status BOOLEAN
,bug_occured BOOLEAN
,session_projects_added INT
,session_likes_given INT
,session_comments_given INT
,inactive_duration INT
,bugs_in_session INT
,session_duration INT
);

--copy data from CSV file
copy public.analysis from 'D:\Showwcase\Sheet\showwcase_sessions.csv' with csv header;

--delete NULL rows
delete from public.analysis
where session_id is NULL;

--set primary key 
alter table public.analysis
add primary key(session_id);

-- Number of new users in October
select count(distinct customer_id)
from public.analysis
where to_char(login_date, 'MM') = '10' 
order by 1;

--Retained user's engagement level (session count > 1)
--included bug count to find correlation of user engagement with the number of bugs.

with A as ( select customer_id, count(session_id) as Session_Count
			from public.analysis
			group by customer_id having count(session_id) > 1
		  )
select distinct A.customer_id, 
				A.Session_Count, 
				sum(session_projects_added) over (partition by A.customer_id) as Total_Projects,
				sum(session_likes_given) over (partition by A.customer_id) as Total_Likes_Given,
				sum(session_comments_given) over (partition by A.customer_id) as Total_Comments_Given,
				(sum(session_duration) over (partition by A.customer_id) - sum(inactive_duration) over (partition by A.customer_id))/60  as Active_Duration_minutes,
				sum(bugs_in_session) over (partition by A.customer_id) as Total_Bugs_count
from public.analysis join A on public.analysis.customer_id = A.customer_id
order by A.session_count desc;

-- users with less session count but encountered more bugs
select 	customer_id,
		count(session_id) as session_count,
		sum(bugs_in_session) as bugs_count
from public.analysis
group by customer_id having count(Session_id) <= 5 and sum(bugs_in_Session) != 0;

--Presence of bugs in a session might contribute to user attrition or might reduce user engagement.
--This query to find increase/decrease in bugs compared to previous day, can be used after major updates for evaluation

with A as (select distinct login_Date, sum(bugs_in_session) over (partition by login_date) as Total_bugs_per_day
		   from public.analysis		 
		  )	,
 B as (select distinct login_date, lag(sum(bugs_in_session),1) over (order by login_date) as lag_value	
		   from public.analysis
		   group by login_date
		   )
select distinct public.analysis.login_date, A.Total_bugs_per_day, A.Total_bugs_per_day - B.lag_value as difference_from_previous_day
from public.analysis join A on A.login_Date = public.analysis.login_date
					join B on A.login_date = B.login_date
order by 1;	

-- Count of users every day in the month of October (can be extended to month, quarter and year with larger dataset)
-- Query to estimate increase or decrease in the number of users per day 
select extract(day from login_date) as "Date_month",
	   count(distinct customer_id) as "Number of Users"
from public.analysis
group by (extract(day from login_date))
order by 1;

--users with no engagement in October 
--Low engagement causes can be analysed and steps can be taken to increase user engagement
select customer_id 
from public.analysis
where projects_added = 'FALSE' 
	and likes_given ='FALSE' 
	and comment_given ='FALSE'
	and to_char(login_date, 'MM') = '10';

--Bounce rate in October
select concat(((select count(customer_id)
		from public.analysis
		where projects_added = 'FALSE' 
				and likes_given ='FALSE' 
				and comment_given ='FALSE'
		)* 100 )/ count(distinct A.customer_id) , '%') as Bounce_Rate
from public.analysis A
where to_char(login_date, 'MM') = '10';

--Query to find users who publish projects but do not like or comment
--user retention found by the count of recurring sessions, is also low
select customer_id, count(session_id) over (partition by customer_id) as session_count
from public.analysis
where projects_added ='TRUE'
and likes_given = 'FALSE'
and comment_given='FALSE'
and to_char(login_date, 'MM') = '10';


--User engagement is proportional to the number of projects added.
--This query ranks users based on number of projects created. These users are the main contributors to the platform.
with A as ( select distinct customer_id,
		   		   sum(session_projects_Added) over (partition by customer_id) as total_projects
		    from public.analysis
		  )
select distinct A.customer_id, A.total_projects, dense_Rank () over (order by A.total_projects desc) as Projects_Rank
from public.analysis join A on public.analysis.customer_id = A.customer_id
order by 3 ;
		
	
--Query to find active duration vs inactive duration 
--Only included values where session_duration > inactive_duration and users with no idle time

select A.customer_id, concat (A.active_duration * 100/A.Total_duration , '%') as Active_percentage, concat(A.Inactivity * 100/A.Total_duration,'%') as Inactive_percentage
from (
select customer_id, sum(session_duration) as Total_duration, sum(session_duration) - sum(inactive_duration) as Active_duration, sum(inactive_duration) as Inactivity
from public.analysis
where session_duration > inactive_duration
group by customer_id
	) A
where A.Inactivity > 0 
order by active_percentage desc;







	
