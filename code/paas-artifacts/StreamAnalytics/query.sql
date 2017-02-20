WITH NoDuplicateMessages AS
(
SELECT
    -- id=message_id, di=device_id, dt=device_time, c=category, m1=measure1, m2=measure2
    id, di, dt, c, m1, m2, COUNT(*) AS dummy
FROM
    [iothub] TIMESTAMP BY DATEADD(millisecond, dt, '1970-01-01T00:00:00Z')
GROUP BY id, di, dt, c, m1, m2, TumblingWindow(second, 5)
)
SELECT
    System.TimeStamp AS wt, di, c, SUM(m1) AS sm1, SUM(m2) AS sm2, count(*) AS nbevents
INTO
    [docdb]
FROM
   NoDuplicateMessages
GROUP BY
    di, c, TumblingWindow(second, 5)

