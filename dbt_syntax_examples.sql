{{ config(
    materialized = 'incremental',
    unique_key = 'order_id',
    on_schema_change='sync_all_columns'
    )
}}


{% set rsrc_list=["x", "y", "z", "unknown"] %}


select
    orders.order_id                                                                                             as order_id,
    sum(pings)                                                                                                  as total_pings,
    min(first_signal_at)                                                                                        as first_signal_at,
    max(last_signal_at)                                                                                         as last_signal_at,
    listagg(distinct rsrc, ', ')                                                                                as rsrc_list,
    --tr/2021-01-26: business requirement to only recognize trackers for orders that are dispatched.
    iff(count(rsrc)>0 and orders.order_state not in ('NEW', 'REGISTERED'), true, false)                         as is_tracker_assigned,
    listagg(distinct tracker_name,', ')                                                                         as tracker_list,

    {% for rsrc in rsrc_list -%}
            sum(iff(rsrc = '{{rsrc}}', pings, null))                                                            as total_{{rsrc}}_pings,
            min(iff(rsrc = '{{rsrc}}', first_signal_at, null))                                                  as first_{{rsrc}}_signal_at,
            max(iff(rsrc = '{{rsrc}}', last_signal_at, null))                                                   as last_{{rsrc}}_signal_at,
            case when sum(iff(rsrc = '{{rsrc}}', pings, null)) >= 4 then 'TRACKED'
                 when sum(iff(rsrc = '{{rsrc}}', pings, null)) between 1 and 3 then 'ERROR'
                 when sum(iff(rsrc = '{{rsrc}}', pings, null)) = 0, 'NOT_TRACKED'
            else 'N/A' end                                                                                      as {{rsrc}}_tracking_status,
	{% endfor -%}

	sum(iff(rsrc <> 'x', pings, null)) 																		    as total_non_x_pings

from {{ ref('y') }} orders

left join {{ source('x', 'abc') }} transfer
    on orders.transfer_id = transfer.id

left join staging
    on orders.transfer_id = staging.transfer_id

{% if is_incremental() %}
    where
    -- this filter will only be applied on an incremental run
		    datediff('day', transfer.start_time::date, current_date) < 14
    {% endif %}

group by
        orders.order_id,
        orders.order_state,
        staging.num_buckets