with events as (
    select customer_id, 
          event, 
          time,
          if(like(value,'%amount%'),toFloat32(trim(trailing'}' from trim(leading'{\'amount: 'from value))),null) amount,
          if(like(value,'%reward%'),toInt32(replace(substring(value,60,2),'}','')),null) reward,
          if(like(value,'%offer%'),substring(value,15,32),null)event_offer_id
    from cafe.events
), t0 as (
    select e1.customer_id, 
           e1.event, 
           e1.time, 
           e1.reward, 
           e1.event_offer_id, 
           e1.amount,
           max( if(e1.event='transaction',e1.time,0) ) over (partition by e1.customer_id, e1.event) max_time,
           count(e1.amount) over (partition by e1.customer_id) count_transaction,
           sum( if(e1.event='transaction',e1.amount,0) ) over (partition by e1.customer_id) sum_amount
    from events e1
), t1 as (
    select customer_id, 
          max(max_time) RR, 
          max (count_transaction) FF, 
          max (sum_amount) MM, 
          sum(reward) sum_reward
    from t0
    group by customer_id
), tc as (
    select *, 
          quantile(0.25)(RR) over() Rc1,
          quantile(0.75)(RR) over() Rc2,
          quantile(0.25)(FF) over() Fc1,
          quantile(0.75)(FF) over() Fc2,
          quantile(0.25)(MM) over() Mc1,
          quantile(0.75)(MM) over() Mc2
    from t1)
select customer_id, 
      FF as count_transaction, 
      MM as sum_amount, 
      if (isnull(sum_reward),0,sum_reward) sum_reward, 
      concat(
              multiIf(RR <= Rc1,'0',RR >= Rc2,'2','1') ,
              multiIf(FF <= Fc1,'0',FF >= Fc2,'2','1') ,
              multiIf(MM <= Mc1,'0',MM >= Mc2,'2','1')  ) RFM
from tc
