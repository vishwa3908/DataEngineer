/*markdown

### Question 
we need to find player with no of gold medals won by them only for players who won only gold medals.


*/

-- CREATE TABLE events (
-- ID int,
-- event varchar(255),
-- YEAR INt,
-- GOLD varchar(255),
-- SILVER varchar(255),
-- BRONZE varchar(255)
-- );


-- INSERT INTO events VALUES (1,'100m',2016, 'Amthhew Mcgarray','donald','barbara');
-- INSERT INTO events VALUES (2,'200m',2016, 'Nichole','Alvaro Eaton','janet Smith');
-- INSERT INTO events VALUES (3,'500m',2016, 'Charles','Nichole','Susana');
-- INSERT INTO events VALUES (4,'100m',2016, 'Ronald','maria','paula');
-- INSERT INTO events VALUES (5,'200m',2016, 'Alfred','carol','Steven');
-- INSERT INTO events VALUES (6,'500m',2016, 'Nichole','Alfred','Brandon');
-- INSERT INTO events VALUES (7,'100m',2016, 'Charles','Dennis','Susana');
-- INSERT INTO events VALUES (8,'200m',2016, 'Thomas','Dawn','catherine');
-- INSERT INTO events VALUES (9,'500m',2016, 'Thomas','Dennis','paula');
-- INSERT INTO events VALUES (10,'100m',2016, 'Charles','Dennis','Susana');
-- INSERT INTO events VALUES (11,'200m',2016, 'jessica','Donald','Stefeney');
-- INSERT INTO events VALUES (12,'500m',2016,'Thomas','Steven','Catherine');

select * from events;
select gold as player,count(gold) from events
where gold not in (select silver from events union all select bronze from events)
group by gold;
select gold as player,count(gold) from events
where gold not in (select silver from events ) and gold not in (select bronze from events)
group by gold;




