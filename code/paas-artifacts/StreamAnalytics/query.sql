WITH NoDuplicateMessages AS
(
SELECT
    -- id=message_id, di=device_id, dt=device_time, c=category, m1=measure1, m2=measure2
    DISTINCT id, di, dt, c, m1, m2
FROM
    [iothub] TIMESTAMP BY DATEADD(millisecond, dt, '1970-01-01T00:00:00Z')
)
SELECT
    -- id=message_id, di=device_id, dt=device_time, c=category, m1=measure1, m2=measure2
    System.TimeStamp AS wt, di, c, SUM(m1) AS sm1, SUM(m2) AS sm2
INTO
    [docdb]
FROM
   NoDuplicateMessages
GROUP BY
    di, c, TumblingWindow(second, 5)

