--create views in business vault
CREATE VIEW Enterprise.calObservation AS
SELECT ObservationId, ObservationChannel, (SELECT SUM(s) FROM UNNEST(ObservationValue) s) AS SumValue, (SELECT AVG(s1) FROM UNNEST(ObservationValue) s1) AS AvgValue
FROM Enterprise.SatObservationValue;

CREATE VIEW Enterprise.Observationformat AS
SELECT ObservationId, Source, TimeStamp,
		CASE
			WHEN ObservationName LIKE '%MSE' THEN 'raw'
			WHEN ObservationName LIKE '%Deoxy' or ObservationName LIKE '%Oxy' THEN 'Hb'
			WHEN ObservationId IN ( SELECT ObservationId 
									FROM Enterprise.SatObservationValue 
									WHERE ObservationChannel LIKE 'H%') THEN 'Hb'
			WHEN ObservationId IN ( SELECT ObservationId 
									FROM Enterprise.SatObservationValue 
									WHERE ObservationChannel LIKE 'raw%') THEN 'raw'
		END format
FROM Enterprise.SatObservationName;

CREATE VIEW Enterprise.type AS
SELECT ObservationId, ObservationChannel,
		CASE
			WHEN ObservationChannel LIKE '%(6%' THEN 'wvl-1'
			WHEN ObservationChannel LIKE '%(8%' THEN 'wvl-2'
			WHEN ObservationChannel LIKE 'raw-2%' THEN 'wvl-2'
			WHEN ObservationChannel LIKE 'raw-1%' THEN 'wvl-1'
			WHEN ObservationId IN ( SELECT ObservationId 
									FROM Enterprise.SatObservationName 
									WHERE ObservationName LIKE '%Deoxy') AND ObservationChannel LIKE 'CH%' THEN 'HbR'
			WHEN ObservationId IN (
									SELECT ObservationId
									FROM Enterprise.SatObservationName 
									WHERE ObservationName LIKE '%Oxy') AND ObservationChannel LIKE 'CH%' THEN 'HbO2'
			WHEN ObservationChannel LIKE 'HbO2%' THEN 'HbO2'
			WHEN ObservationChannel LIKE 'HbR%' THEN 'HbR'
		END format
FROM Enterprise.SatObservationValue;

--create tables in information mart
--observation fact and dimension tables
CREATE TABLE Information.FactObservation
AS (SELECT a.ObservationId, a.format, b.format AS type, b.ObservationChannel, c.MetaDataId,f.AvgValue, e.CollectedAtSession AS SessionId
   FROM Enterprise.Observationformat AS a
   JOIN Enterprise.type AS b
   ON a.ObservationId = b.ObservationId
   JOIN Enterprise.ObservationMetaData AS c
   ON a.ObservationId = c.ObservationId
   JOIN Enterprise.HubObservation AS e
   ON a.ObservationId = e.ObservationId
   JOIN Enterprise.CalObservation AS f
   ON a.ObservationId = f.ObservationId
   WHERE b.ObservationChannel = f.ObservationChannel);


CREATE TABLE Information.DimensionObsValue
AS (SELECT ObservationId, ObservationChannel, ObservationValue, ObservationTimeStamp
    FROM Enterprise.SatObservationValue);

CREATE TABLE Information.DimensionObsName
AS (SELECT ObservationId, ObservationName
    FROM Enterprise.SatObservationName);

CREATE TABLE Information.DimensionMetaDataKeyValuePair
AS (SELECT MetaDataId, key, value
    FROM Enterprise.SatMetaDataKeyValuePair);

CREATE TABLE Information.DimensionSessionName
AS(SELECT  SessionId, SessionName
	FROM Enterprise.SatSessionName);

-- subject fact and dimension tables
CREATE TABLE Information.FactSubject 
AS(SELECT a.experimentalunitid, a.groupid, a.sessionid, b.observationid, c.metadataid,  d.experimentid
FROM enterprise.attendssession as a
JOIN enterprise.hubobservation as b
ON a.sessionid = b.collectedatsession
JOIN enterprise.observationmetadata AS c
ON b.observationid = c.observationid
JOIN enterprise.participatesin AS d
ON a.experimentalunitid = d.experimentalunitid);

CREATE TABLE Information.DimensionGroupName
AS(SELECT  GroupId, GroupName
	FROM Enterprise.SatGroupName);

CREATE TABLE Information.DimensionSubjectName
AS(SELECT  SubjectId, SubjectName
	FROM Enterprise.HubSubject);

CREATE TABLE Information.DimensionExperimentTitle
AS(SELECT  ExperimentId, ExperimentTitle
	FROM Enterprise.SatExperimentTitle);

-- experiment fact and dimension tables
Create table Information.FactExperiment AS 
(SELECT a.treatmentid, b.experimentid, c.factorid, d.groupid
FROM Enterprise.SatTreatmentFactorLevel as a
JOIN Enterprise.hubtreatment as b
ON a.treatmentid = b.treatmentid
JOIN Enterprise.HubFactor as c
ON a.TreatmentFactorLevel = c.FactorId
JOIN Enterprise.HubGroup as d
On a.treatmentid = d.treatmentid);

CREATE TABLE Information.DimensionSubjectGroup AS
(SELECT t.ExperimentTitle, g.GroupName, u.SubjectName, g.GroupId, u.SubjectId
 FROM Information.FactSubject AS q
JOIN Enterprise.SatExperimentTitle AS t
ON t.ExperimentId = q.ExperimentId
JOIN Enterprise.SatGroupName AS g
ON g.GroupId = q.GroupId
JOIN Enterprise.HubSubject AS u
ON u.SubjectId = q.ExperimentalUnitId
);

Create table Information.DimensionExperimentFactor 
AS(SELECT b.ExperimentTitle, a.TreatmentId, c.FactorName, d.LevelValue
FROM Information.FactExperiment AS a
JOIN Enterprise.SatExperimentTitle AS b
ON a.ExperimentId = b.ExperimentId
JOIN Enterprise.SatFactorName AS c
ON a.FactorId = c.FactorId
JOIN Enterprise.SatFactorLevel AS d
ON a.FactorId = d.FactorId);

--Q1 view
CREATE VIEW Information.question1 AS
SELECT e.SubjectName, a.format, a.type,d.ObservationName, a.ObservationChannel, UNNEST(c.ObservationValue) AS value, UNNEST(c.ObservationTimeStamp) AS time
FROM Information.FactObservation AS a
JOIN Information.FactSubject AS b
ON a.ObservationId = b.ObservationId
JOIN Information.DimensionObsValue AS c
ON a.ObservationId = c.ObservationId
JOIN Information.DimensionObsName AS d
ON a.ObservationId = d.ObservationId
JOIN Information.DimensionSubjectName AS e
ON b.ExperimentalUnitId = e.SubjectId
WHERE a.ObservationChannel = c.ObservationChannel AND type IS NOT NULL;
--Q2 view
CREATE VIEW Information.question2 AS
SELECT a.type, a.ObservationChannel, e.GroupName, a.AvgValue
FROM Information.FactObservation AS a
JOIN Information.FactSubject AS b
ON a.ObservationId = b.ObservationId
JOIN Information.DimensionGroupName AS e
ON b.GroupId = e.GroupId
WHERE a.format = 'Hb' AND type IS NOT NULL 
ORDER BY GroupName, ObservationChannel;
--Q3 view
CREATE VIEW Information.question3 AS
SELECT DISTINCT * FROM Information.DimensionExperimentFactor;

--Q4 view
CREATE VIEW Information.question4 AS
SELECT DISTINCT * FROM Information.DimensionSubjectGroup;

--Q5 view
CREATE VIEW Information.question5 AS
SELECT a.format, a.type, c.key, c.value, d.ObservationValue
FROM Information.FactObservation AS a
JOIN Information.FactSubject AS b
ON a.ObservationId = b.ObservationId
JOIN Information.DimensionMetaDataKeyValuePair AS c
ON b.MetaDataId = c.MetaDataId
JOIN Information.DimensionObsValue AS d
ON a.ObservationId = d.ObservationId
WHERE a.ObservationChannel = d.ObservationChannel;
--Q6 view
CREATE VIEW Information.question6 AS
SELECT a.type, e.SubjectName, d.ObservationName, UNNEST(c.ObservationValue) AS value, UNNEST(c.ObservationTimeStamp) AS time
FROM Information.FactObservation AS a
JOIN Information.FactSubject AS b
ON a.ObservationId = b.ObservationId
JOIN Information.DimensionObsValue AS c
ON a.ObservationId = c.ObservationId
JOIN Information.DimensionObsName AS d
ON a.ObservationId = c.ObservationId
JOIN Information.DimensionSubjectName AS e
ON b.ExperimentalUnitId = e.SubjectId
WHERE a.ObservationChannel = c.ObservationChannel AND a.format = 'Hb' AND type IS NOT NULL; 

--Q7 view
CREATE VIEW Information.question7 AS
SELECT a.type, e.GroupName, d.ObservationName, UNNEST(c.ObservationValue) AS value, UNNEST(c.ObservationTimeStamp) AS time
FROM Information.FactObservation AS a
JOIN Information.FactSubject AS b
ON a.ObservationId = b.ObservationId
JOIN Information.DimensionObsValue AS c
ON a.ObservationId = c.ObservationId
JOIN Information.DimensionObsName AS d
ON a.ObservationId = c.ObservationId
JOIN Information.DimensionGroupName AS e
ON b.GroupId = e.GroupId
WHERE a.ObservationChannel = c.ObservationChannel AND a.format = 'Hb' AND type IS NOT NULL; 
