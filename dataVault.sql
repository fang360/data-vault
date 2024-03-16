CREATE DATABASE smdvault;
\c smdvault
CREATE SCHEMA Staging;
CREATE SCHEMA Enterprise;
CREATE SCHEMA Information;

--create staging table
CREATE TABLE IF NOT EXISTS Staging.SatObservationName(  
        ONId SERIAL PRIMARY KEY,      
        ObservationId TEXT NOT NULL,
        TimeStamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
        Source VARCHAR ( 225 ) NOT NULL,
        ObservationName VARCHAR(40)
    );
CREATE TABLE IF NOT EXISTS Staging.SatObservationValue( 
        OVId SERIAL PRIMARY KEY,       
        ObservationId TEXT NOT NULL,
        TimeStamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
        Source VARCHAR ( 225 ) NOT NULL,
        ObservationChannel VARCHAR(40),
        ObservationValue NUMERIC [],
        ObservationTimeStamp NUMERIC []
    );
CREATE TABLE IF NOT EXISTS Staging.SatMetaDataKeyValuePair(   
        MVId SERIAL PRIMARY KEY,     
        MetaDataId TEXT NOT NULL,
        TimeStamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
        Source VARCHAR ( 225 ) NOT NULL,
        Key VARCHAR(40) NOT NULL,
        Value TEXT,
        DataType VARCHAR(15)
    );





--CREATE TABLE IF NOT EXISTS inside the database
/* Tables include:
    HubSubject
        SatSubjectAge
        SatSubjectName
    HubExperimentalUnit
    HubExperiment
        SatExperimentAcronym
        SatExperimentTitle
    HubFactor
        SatFactorName
        SatFactorLevel
    HubTreatment
        SatTreatmentFactorLevel
    HubGroup
        SatGroupName
    AssignedTo
    ParticipatesIn
        SatExperimentalUnitIdentifier
    HubSession
        SatSessionName
    HubObservation
        SatObservationName
        SatObservationValue
    HubMetaData
        SatMetaDataKeyValuePair
    SessionMetaData
    ObservationMetaData
*/



CREATE TABLE IF NOT EXISTS Enterprise.HubSubject(        
        SubjectId TEXT UNIQUE NOT NULL,
    	TimeStamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
    	Source VARCHAR ( 225 ) NOT NULL,
        SubjectName VARCHAR(40),
        PRIMARY KEY (SubjectId, TimeStamp, Source)
    );
CREATE TABLE IF NOT EXISTS Enterprise.SatSubjectAge (        
        SubjectId TEXT NOT NULL,
    	TimeStamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
    	Source VARCHAR ( 225 ) NOT NULL,
        SubjectAge SMALLINT,
        PRIMARY KEY (SubjectId, TimeStamp, Source),
        FOREIGN KEY (SubjectId )
            REFERENCES Enterprise.HubSubject (SubjectId )
    );
CREATE TABLE IF NOT EXISTS Enterprise.SatSubjectName (        
        SubjectId TEXT NOT NULL,
    	TimeStamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
    	Source VARCHAR ( 225 ) NOT NULL,
        SubjectName VARCHAR(40),
        PRIMARY KEY (SubjectId, TimeStamp, Source),
        FOREIGN KEY (SubjectId )
            REFERENCES Enterprise.HubSubject (SubjectId )
    );

CREATE TABLE IF NOT EXISTS Enterprise.HubExperimentalUnit (        
        ExperimentalUnitId TEXT UNIQUE NOT NULL,
    	TimeStamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
    	Source VARCHAR ( 225 ) NOT NULL,
        PRIMARY KEY (ExperimentalUnitId, TimeStamp, Source)
    );
CREATE TABLE IF NOT EXISTS Enterprise.HubExperiment (        
        ExperimentId TEXT UNIQUE NOT NULL,
    	TimeStamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
    	Source VARCHAR ( 225 ) NOT NULL,
        PRIMARY KEY (ExperimentId, TimeStamp, Source)
    );
CREATE TABLE IF NOT EXISTS Enterprise.SatExperimentAcronym(        
        ExperimentId TEXT NOT NULL,
    	TimeStamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
    	Source VARCHAR ( 225 ) NOT NULL,
        ExperimentAcronym VARCHAR(15),
        PRIMARY KEY (ExperimentId, TimeStamp, Source),
        FOREIGN KEY (ExperimentId )
            REFERENCES Enterprise.HubExperiment  (ExperimentId )
    );
CREATE TABLE IF NOT EXISTS Enterprise.SatExperimentTitle(        
        ExperimentId TEXT NOT NULL,
    	TimeStamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
    	Source VARCHAR ( 225 ) NOT NULL,
        ExperimentTitle VARCHAR(225),
        PRIMARY KEY (ExperimentId, TimeStamp, Source),
        FOREIGN KEY (ExperimentId )
            REFERENCES Enterprise.HubExperiment  (ExperimentId )
    );
CREATE TABLE IF NOT EXISTS Enterprise.HubFactor(        
        FactorId TEXT UNIQUE NOT NULL,
    	TimeStamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
    	Source VARCHAR ( 225 ) NOT NULL,
        ExperimentId TEXT NOT NULL,
        IsCofactor BOOLEAN DEFAULT 'f',
        PRIMARY KEY (FactorId, TimeStamp, Source),
        FOREIGN KEY (ExperimentId )
            REFERENCES Enterprise.HubExperiment  (ExperimentId )
    );
CREATE TABLE IF NOT EXISTS Enterprise.SatFactorName(        
        FactorId TEXT NOT NULL,
    	TimeStamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
    	Source VARCHAR ( 225 ) NOT NULL,
        FactorName VARCHAR(40),
        PRIMARY KEY (FactorId, TimeStamp, Source),
        FOREIGN KEY (FactorId )
            REFERENCES Enterprise.HubFactor (FactorId )
    );
CREATE TABLE IF NOT EXISTS Enterprise.SatFactorLevel(        
        FactorId TEXT NOT NULL,
    	TimeStamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
    	Source VARCHAR ( 225 ) NOT NULL,
        LevelValue VARCHAR(40),
        PRIMARY KEY (FactorId, TimeStamp, Source),
        FOREIGN KEY (FactorId)
            REFERENCES Enterprise.HubFactor (FactorId)
    );
CREATE TABLE IF NOT EXISTS Enterprise.HubTreatment(        
        TreatmentId TEXT UNIQUE NOT NULL,
    	TimeStamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
    	Source VARCHAR ( 225 ) NOT NULL,
        ExperimentId TEXT NOT NULL,
        PRIMARY KEY (TreatmentId, TimeStamp, Source),
        FOREIGN KEY (ExperimentId )
            REFERENCES Enterprise.HubExperiment  (ExperimentId )
    );
CREATE TABLE IF NOT EXISTS Enterprise.SatTreatmentFactorLevel(        
        TreatmentId TEXT NOT NULL,
    	TimeStamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
    	Source VARCHAR ( 225 ) NOT NULL,
        TreatmentFactorLevel TEXT,
        PRIMARY KEY (TreatmentId, TimeStamp, Source),
        FOREIGN KEY (TreatmentId )
            REFERENCES Enterprise.HubTreatment (TreatmentId )
    );
CREATE TABLE IF NOT EXISTS Enterprise.HubGroup(        
        GroupId TEXT UNIQUE NOT NULL,
    	TimeStamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
    	Source VARCHAR ( 225 ) NOT NULL,
        TreatmentId TEXT NOT NULL,
        PRIMARY KEY (GroupId, TimeStamp, Source),
        FOREIGN KEY (TreatmentId )
            REFERENCES Enterprise.HubTreatment (TreatmentId )
    );
CREATE TABLE IF NOT EXISTS Enterprise.SatGroupName(        
        GroupId TEXT NOT NULL,
    	TimeStamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
    	Source VARCHAR ( 225 ) NOT NULL,
        GroupName VARCHAR(40),
        PRIMARY KEY (GroupId, TimeStamp, Source),
        FOREIGN KEY (GroupId )
            REFERENCES Enterprise.HubGroup (GroupId )
    );
CREATE TABLE IF NOT EXISTS Enterprise.AssignedTo(        
        AssignedToId TEXT UNIQUE NOT NULL,
    	TimeStamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
    	Source VARCHAR ( 225 ) NOT NULL,
        ExperimentalUnitId TEXT NOT NULL,
        GroupId TEXT NOT NULL,
        PRIMARY KEY (AssignedToId, TimeStamp, Source),
        FOREIGN KEY (ExperimentalUnitId)
            REFERENCES Enterprise.HubExperimentalUnit (ExperimentalUnitId),
        FOREIGN KEY (GroupId)
            REFERENCES Enterprise.HubGroup (GroupId)
    );
CREATE TABLE IF NOT EXISTS Enterprise.ParticipatesIn(        
        ParticipatesInId TEXT UNIQUE NOT NULL,
    	TimeStamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
    	Source VARCHAR ( 225 ) NOT NULL,
        ExperimentalUnitId TEXT NOT NULL,
        ExperimentId TEXT NOT NULL,
        PRIMARY KEY (ParticipatesInId, TimeStamp, Source),
        FOREIGN KEY (ExperimentId)
            REFERENCES Enterprise.HubExperiment (ExperimentId),
        FOREIGN KEY (ExperimentalUnitId)
            REFERENCES Enterprise.HubExperimentalUnit (ExperimentalUnitId)
    );
CREATE TABLE IF NOT EXISTS Enterprise.SatExperimentalUnitIdentifier(        
        ParticipatesInId TEXT NOT NULL,
    	TimeStamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
    	Source VARCHAR ( 225 ) NOT NULL,
        ExperimentalUnitIdentifierId VARCHAR(15),
        PRIMARY KEY (ParticipatesInId, TimeStamp, Source),
        FOREIGN KEY (ParticipatesInId)
            REFERENCES Enterprise.ParticipatesIn (ParticipatesInId)
    );
CREATE TABLE IF NOT EXISTS Enterprise.HubSession(        
        SessionId TEXT UNIQUE NOT NULL,
    	TimeStamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
    	Source VARCHAR ( 225 ) NOT NULL,
        PRIMARY KEY (SessionId, TimeStamp, Source)
    );
CREATE TABLE IF NOT EXISTS Enterprise.SatSessionName(        
        SessionId TEXT NOT NULL,
    	TimeStamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
    	Source VARCHAR ( 225 ) NOT NULL,
        SessionName VARCHAR(40),
        PRIMARY KEY (SessionId, TimeStamp, Source),
        FOREIGN KEY (SessionId)
            REFERENCES Enterprise.HubSession (SessionId)
    );
CREATE TABLE IF NOT EXISTS Enterprise.HubObservation(        
        ObservationId TEXT UNIQUE NOT NULL,
    	TimeStamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
    	Source VARCHAR ( 225 ) NOT NULL,
        CollectedAtSession TEXT NOT NULL, 
        PRIMARY KEY (ObservationId, TimeStamp, Source),
        FOREIGN KEY (CollectedAtSession)
            REFERENCES Enterprise.HubSession (SessionId)
    );
CREATE TABLE IF NOT EXISTS Enterprise.SatObservationName(        
        ObservationId TEXT NOT NULL,
    	TimeStamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
    	Source VARCHAR ( 225 ) NOT NULL,
        ObservationName VARCHAR(40),
        PRIMARY KEY (ObservationId, TimeStamp, Source),
        FOREIGN KEY (ObservationId )
            REFERENCES Enterprise.HubObservation (ObservationId )
    );
CREATE TABLE IF NOT EXISTS Enterprise.SatObservationValue(        
        ObservationId TEXT NOT NULL,
    	TimeStamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
    	Source VARCHAR ( 225 ) NOT NULL,
        ObservationChannel VARCHAR(40),
        ObservationValue NUMERIC [],
        ObservationTimeStamp NUMERIC [],
        PRIMARY KEY (ObservationId, TimeStamp, Source),
        FOREIGN KEY (ObservationId )
            REFERENCES Enterprise.HubObservation (ObservationId )
    );
CREATE TABLE IF NOT EXISTS Enterprise.HubMetaData(        
        MetaDataId TEXT UNIQUE NOT NULL,
    	TimeStamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
    	Source VARCHAR ( 225 ) NOT NULL,
        PRIMARY KEY (MetaDataId, TimeStamp, Source)
    );
CREATE TABLE IF NOT EXISTS Enterprise.SatMetaDataKeyValuePair(        
        MetaDataId TEXT NOT NULL,
    	TimeStamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
    	Source VARCHAR ( 225 ) NOT NULL,
        Key VARCHAR(40) NOT NULL,
        Value TEXT,
		DataType VARCHAR(15),
        PRIMARY KEY (MetaDataId, TimeStamp, Source),
        FOREIGN KEY (MetaDataId )
            REFERENCES Enterprise.HubMetaData (MetaDataId )
    );
CREATE TABLE IF NOT EXISTS Enterprise.SessionMetaData(        
        SessionMetaDataId TEXT UNIQUE NOT NULL,
    	TimeStamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
    	Source VARCHAR ( 225 ) NOT NULL,
        SessionId TEXT NOT NULL,
        MetaDataId TEXT NOT NULL,
        PRIMARY KEY (SessionMetaDataId, TimeStamp, Source),
        FOREIGN KEY (SessionId)
            REFERENCES Enterprise.HubSession (SessionId),
        FOREIGN KEY (MetaDataId)
            REFERENCES Enterprise.HubMetaData (MetaDataId)
    );
CREATE TABLE IF NOT EXISTS Enterprise.ObservationMetaData(        
        ObservationMetaDataId TEXT UNIQUE NOT NULL,
    	TimeStamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
    	Source VARCHAR ( 225 ) NOT NULL,
        ObservationId TEXT NOT NULL,
        MetaDataId TEXT NOT NULL,
        PRIMARY KEY (ObservationMetaDataId, TimeStamp, Source),
        FOREIGN KEY (ObservationId )
            REFERENCES Enterprise.HubObservation (ObservationId ),
        FOREIGN KEY (MetaDataId )
            REFERENCES Enterprise.HubMetaData (MetaDataId )
    );
CREATE TABLE IF NOT EXISTS Enterprise.AttendsSession(        
        AttendsSessionId TEXT UNIQUE NOT NULL,
    	TimeStamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
    	Source VARCHAR ( 225 ) NOT NULL,
        ExperimentalUnitId TEXT,
        GroupId TEXT NOT NULL,
        SessionId TEXT NOT NULL,
        PRIMARY KEY (AttendsSessionId, TimeStamp, Source),
        FOREIGN KEY (ExperimentalUnitId )
            REFERENCES Enterprise.HubExperimentalUnit (ExperimentalUnitId ),
        FOREIGN KEY (GroupId )
            REFERENCES Enterprise.HubGroup (GroupId ),
        FOREIGN KEY (SessionId )
            REFERENCES Enterprise.HubSession (SessionId )
    );

