--Получение таблицы events
with events as (
    select customer_id, 
    event, 
    time,
    if(like(value,'%amount%'),toFloat32(trim(trailing'}' from trim(leading'{\'amount: 'from value))),null)amount,
    if(like(value,'%reward%'),toInt32(replace(substring(value,60,2),'}','')),null)reward,
    if(like(value,'%offer%'),substring(value,15,32),null)event_offer_id
    from cafe.events
)
select e1.customer_id, 
      e1.event, 
      e1.time, 
      e1.reward, 
      e1.event_offer_id, 
      e2.amount amount
from events e1
left join (select customer_id, time, amount from events where event='transaction') e2 
on (e1.customer_id=e2.customer_id) and (e1.time=e2.time) and (e1.event='offer completed' or  e1.event='transaction')


--Получение таблицы customers
SELECT customer_id, 
      toDate(toString(became_member_on)) member_date,
      extract(year from toDate(toString(became_member_on))) member_year,
      if(gender='M',1,0) male,
      if(gender='F',1,0) female,
      if(gender='O',1,0) other,
      if(age=118,1,null) age118,
      if(age<>118,age,null) age,
      if(income=0,null,income) income
from cafe.customers 


--Получение таблицы offers
SELECT *, 
      countMatches(channels,';')+1 channels_count, 
      countMatches(channels,'web') web, 
      countMatches(channels,'email') email,
      countMatches(channels,'mobile') mobile,
      countMatches(channels,'social') social
FROM cafe.offers
