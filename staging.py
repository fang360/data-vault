# -*- coding: utf-8 -*-
"""
Created on Sun Nov 20 16:48:16 2022

@author: jigme
"""
import pandas as pd
import time
import numpy as np
import os
import psycopg2
import re
import hashlib

def Postgres(command,db):
    conn = None
    try:
        conn = psycopg2.connect(db)
        cur = conn.cursor()
        
        cur.execute(command)
        # close communication WITH the PostgreSQL database server
        cur.close()
        # commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
def staging():
    #TRUNCATE tables in the PostgreSQL database
    commands = (
        """
            TRUNCATE TABLE Staging.SatObservationName, Staging.SatObservationValue, Staging.SatMetaDataKeyValuePair;
        """)
    return commands           
def InsertByType(EntityList):
    #insert multiple data into table
    Values = str(EntityList[1:]).strip('[]')
    sql = "INSERT INTO "+ EntityList[0]+" VALUES"+Values
    return sql

def TypeData(db):
    #type and hash data
    HashKey = ['Visuomotorfunctionalconnectivity','Pre-autism','VM|visual|present','VM|visual|notpresent','VM|Motor|present','VM|Motor|notpresent','Autism|Syllabal|present','Autism|Syllabal|notpresent','VM|01','VM|02','VM|03','VM|04','Autism|05','Autism|06','VM|VisualMotorTask','VM|VisualOnly','VM|MotorOnly','VM|Reset','Autism|stressedconversation','Autism|normalconversation']
    
    for i in range(len(HashKey)):
        HashKey[i] = hashlib.sha1(HashKey[i].encode()).hexdigest()
    
    experiment = ['Enterprise.HubExperiment(ExperimentId, Source)',(HashKey[0],'VMData_Blinded'),(HashKey[1],'PreAutismData_Blinded')]
    SatExpTitle = ['Enterprise.SatExperimentTitle(ExperimentId, Source, ExperimentTitle)',(HashKey[0],'VMData_Blinded','Visuomotor functional connectivity'),(HashKey[1],'PreAutismData_Blinded','Pre-autism')]
    SatExpAcr = ['Enterprise.SatExperimentAcronym(ExperimentId, Source, ExperimentAcronym)',(HashKey[0],'VMData_Blinded','VM'),(HashKey[1],'PreAutismData_Blinded','Autism')]
    HubFactor = ['Enterprise.HubFactor (FactorId, Source, ExperimentId, IsCofactor)', (HashKey[2], 'VMData_Blinded', HashKey[0], False),(HashKey[3], 'VMData_Blinded', HashKey[0], True), (HashKey[4], 'VMData_Blinded', HashKey[0], False), (HashKey[5], 'VMData_Blinded', HashKey[0],True), (HashKey[6], 'PreAutismData_Blinded', HashKey[1], False), (HashKey[7], 'PreAutismData_Blinded', HashKey[1],True)]#IsCofactor
    SatFactorName = ['Enterprise.SatFactorName(FactorId, Source, FactorName)', (HashKey[2], 'VMData_Blinded', 'visual'), (HashKey[3], 'VMData_Blinded', 'visual'), (HashKey[4], 'VMData_Blinded', 'Motor'), (HashKey[5], 'VMData_Blinded', 'Motor'), (HashKey[6], 'PreAutismData_Blinded', 'Syllabal'), (HashKey[7], 'PreAutismData_Blinded', 'Syllabal')]
    SatFactorLevel = ['Enterprise.SatFactorLevel(FactorId, Source, LevelValue)', (HashKey[2], 'VMData_Blinded', 'present'), (HashKey[3], 'VMData_Blinded', 'not present'), (HashKey[4], 'VMData_Blinded', 'present'), (HashKey[5], 'VMData_Blinded', 'not present'), (HashKey[6], 'PreAutismData_Blinded', 'present'), (HashKey[7], 'PreAutismData_Blinded', 'not present')]
    HubTreatment = ['Enterprise.HubTreatment(TreatmentId, Source, ExperimentId)', (HashKey[8], 'VMData_Blinded', HashKey[0]), (HashKey[9], 'VMData_Blinded', HashKey[0]), (HashKey[10], 'VMData_Blinded', HashKey[0]), (HashKey[11], 'VMData_Blinded', HashKey[0]), (HashKey[12], 'PreAutismData_Blinded', HashKey[1]), (HashKey[13], 'PreAutismData_Blinded', HashKey[1])]
    SatTreatmentFactorLevel = ['Enterprise.SatTreatmentFactorLevel(TreatmentId, Source, TreatmentFactorLevel)', (HashKey[8], 'VMData_Blinded', HashKey[2]), (HashKey[9], 'VMData_Blinded', HashKey[2]), (HashKey[10], 'VMData_Blinded', HashKey[3]), (HashKey[11], 'VMData_Blinded', HashKey[3]), (HashKey[12], 'PreAutismData_Blinded', HashKey[6]), (HashKey[13], 'PreAutismData_Blinded', HashKey[7])]
    HubGroup = ['Enterprise.HubGroup(GroupId, Source, TreatmentId)', (HashKey[14], 'VMData_Blinded', HashKey[8]), (HashKey[15], 'VMData_Blinded', HashKey[9]), (HashKey[16], 'VMData_Blinded', HashKey[10]), (HashKey[17], 'VMData_Blinded', HashKey[11]), (HashKey[18], 'PreAutismData_Blinded', HashKey[12]), (HashKey[19], 'PreAutismData_Blinded', HashKey[13])]
    SatGroupName = ['Enterprise.SatGroupName(GroupId, Source, GroupName)', (HashKey[14], 'VMData_Blinded', 'VisualMotor Task'), (HashKey[15], 'VMData_Blinded', 'Visual Only'), (HashKey[16], 'VMData_Blinded', 'Motor Only'), (HashKey[17], 'VMData_Blinded', 'Reset'), (HashKey[18], 'PreAutismData_Blinded', 'stressed conversation'), (HashKey[19], 'PreAutismData_Blinded', 'normal conversation')]
    SatTreatmentFactorLevel1 = ['Enterprise.SatTreatmentFactorLevel(TreatmentId, Source, TreatmentFactorLevel)',(HashKey[8], 'VMData_Blinded', HashKey[4]) , (HashKey[9], 'VMData_Blinded', HashKey[5]) , (HashKey[10], 'VMData_Blinded', HashKey[4]) , (HashKey[11], 'VMData_Blinded', HashKey[5])]
    ExperimentPart = [ experiment, SatExpTitle, SatExpAcr, HubFactor, SatFactorName, SatFactorLevel, HubTreatment, SatTreatmentFactorLevel, HubGroup, SatGroupName] #,SatTreatmentFactorLevel1
    
    for i in ExperimentPart:
        command = InsertByType(i)

        Postgres(command,db)
    time.sleep(3)
    command = InsertByType(SatTreatmentFactorLevel1)
    Postgres(command,db)
    return HashKey

def Insert(TableName, Column, EntityList):
    #insert data into tables
    Values = str(EntityList)
    Column = str(Column).replace("'", " ")
    sql = "INSERT INTO "+ TableName + Column+" VALUES"+Values
    return sql            

def subject(Subject, SubjectHash, db):
    #transform and load subject data
    if type(Subject) == str: #dataset 2
        #INSERT INTO HubSubject
        command = Insert('Enterprise.HubSubject','(SubjectName,Source,SubjectId)', Subject)
        Postgres(command,db)
        #INSERT INTO SatSubjectName
        command = Insert('Enterprise.SatSubjectName','(SubjectName,Source,SubjectId)', Subject)
        Postgres(command,db)
        #insert into HubExperimentalUnit
        Subject = Subject.split(',',1)
        command = Insert('Enterprise.HubExperimentalUnit','(Source, ExperimentalUnitId)', "("+Subject[1])
        Postgres(command,db)
        #insert into ParticipatesIn
        ParticipatesInId = hashlib.sha1((SubjectHash+'Pre-autism').encode()).hexdigest()
        command = Insert('Enterprise.ParticipatesIn','(ParticipatesInId,Source,ExperimentalUnitId, ExperimentId)', "('"+ParticipatesInId+"', "+Subject[1][:-1]+", '"+ HashKey[1] +"')")
        Postgres(command,db)
        
    else: #dataset1
        #INSERT INTO HubSubject  
        HubSubject = Subject.drop(columns=[2])
        value = str(tuple(HubSubject.iloc[1,:]))
        command = Insert('Enterprise.HubSubject','(SubjectName,Source,SubjectId)', value)
        Postgres(command,db)
        #INSERT INTO SatSubjectName
        command = Insert('Enterprise.SatSubjectName','(SubjectName,Source,SubjectId)', value)
        Postgres(command,db)
    
        #INSERT INTO SatSubjectAge
        SatSubjectAge = Subject.drop(columns=[1])
        value = str(tuple(SatSubjectAge.iloc[1,:]))
        command = Insert('Enterprise.SatSubjectAge','(SubjectAge,Source,SubjectId)', value)
        Postgres(command,db)
        
        #insert into HubExperimentalUnit
        Subject = Subject.drop(columns=[1,2])
        value = str(tuple(Subject.iloc[1,:]))
        command = Insert('Enterprise.HubExperimentalUnit','(Source, ExperimentalUnitId)', value)
        Postgres(command,db)
        #insert into ParticipatesIn
        ParticipatesInId = hashlib.sha1((SubjectHash+'VMData_Blinded').encode()).hexdigest()
        value = "('"+str(ParticipatesInId)+"', "+value[1:-1]+ ", '"+ HashKey[0]+"')"
        command = Insert('Enterprise.ParticipatesIn','(ParticipatesInId,Source, ExperimentalUnitId, ExperimentId)', value)
        Postgres(command,db)
    return ParticipatesInId
    
def session(SessionHash, source, SessionName, db):
    #insert into hub and satellite for session
    command = Insert ('Enterprise.HubSession', '(SessionId, Source)', tuple([SessionHash, source]))
    Postgres(command,db)
    command = Insert('Enterprise.SatSessionName', '(SessionId, Source, SessionName)', tuple([SessionHash, source, SessionName]))
    Postgres(command,db)
    
def identifier(ParticipatesInId, Source, ExperimentalUnitIdentifierId, db):
    #insert into SatExperimentalUnitIdentifier
    command = Insert ('Enterprise.SatExperimentalUnitIdentifier', '(ParticipatesInId, Source, ExperimentalUnitIdentifierId)', tuple([ParticipatesInId, Source, ExperimentalUnitIdentifierId]))
    Postgres(command,db)

def meta(MetaData, MetaDataHash, SessionHash, db):
    #insert into metadata related table
    #hub
    if type(MetaData) == str: #.evt file
        
        command = Insert('Enterprise.HubMetaData', '(MetaDataId, Source)',tuple([MetaDataHash, 'PreAutismData_Blinded']))
        Postgres(command,db)
        
        #Link
        command = Insert('Enterprise.SessionMetaData', '(SessionMetaDataId, SessionId, MetaDataId, Source)', tuple([hashlib.sha1((SessionHash+MetaDataHash).encode()).hexdigest(), SessionHash, MetaDataHash, 'PreAutismData_Blinded']))
        Postgres(command,db)
        
        #satellite
        
        value = str(MetaData).strip('[]')
        
        command = Insert('Staging.SatMetaDataKeyValuePair',tuple(['Key','Value','DataType', 'Source', 'MetaDataId']),value)
        Postgres(command,db)
    else: #.hdr file & dataset1
        if MetaData[0][0] == 'ID':
            MetaData = str(MetaData)
            command = Insert('Enterprise.HubMetaData', '(MetaDataId, Source)',tuple([MetaDataHash, 'VMData_Blinded']))
            Postgres(command,db)

            #Link
            command = Insert('Enterprise.SessionMetaData', '(SessionMetaDataId, SessionId, MetaDataId, Source)', tuple([hashlib.sha1((SessionHash+MetaDataHash).encode()).hexdigest(), SessionHash, MetaDataHash, 'VMData_Blinded']))
            Postgres(command,db)
            value = str(MetaData).strip('[]')
            command = Insert('Staging.SatMetaDataKeyValuePair',tuple(['Key','Value','DataType', 'Source', 'MetaDataId']),value)
            Postgres(command,db)
        else:
            MetaData = str(MetaData)
            value = str(MetaData).strip('[]')
            command = Insert('Staging.SatMetaDataKeyValuePair',tuple(['Key','Value','DataType', 'Source', 'MetaDataId']),value)
            Postgres(command,db)       
    
   
def obs(Observation, SessionId, ObservationHash, MetaDataHash, check, db):
    #insert into observation related tables
    if type(Observation) != str: #dataset 1
        #Hub
        command = Insert('Enterprise.HubObservation', '(ObservationId, Source, CollectedAtSession)', tuple([ObservationHash, 'VMData_Blinded', SessionId]))
        Postgres(command,db)
        #Sat
        name = str(Observation.drop(columns=['ObservationChannel','ObservationValue','ObservationTimeStamp']).values.tolist()).replace('[','(').replace(']',')')
        command = Insert('Staging.SatObservationName', '( ObservationName, Source, ObservationId)', name[1:-1])
        Postgres(command,db)
        value = str(Observation.drop(columns=['ObservationName']).values.tolist()).replace('[','(').replace(']',')')
    
        command = Insert('Staging.SatObservationValue', '(ObservationChannel, ObservationValue, ObservationTimeStamp, Source, ObservationId)', value[1:-1])
        Postgres(command,db)

        #Link
        command = Insert('Enterprise.ObservationMetaData', '(ObservationMetaDataId, Source, ObservationId, MetaDataId)', tuple([hashlib.sha1((ObservationHash+MetaDataHash).encode()).hexdigest(), 'VMData_Blinded', ObservationHash, MetaDataHash]))
        Postgres(command,db)
    elif check ==0: #dataset2 first row
        #Hub
        command = Insert('Enterprise.HubObservation', '(ObservationId, Source, CollectedAtSession)', tuple([ObservationHash, 'PreAutismData_Blinded', SessionId]))
        Postgres(command,db)
        #Sat
        name = Observation.split("|")
        
        del name[2:8]

        name =" ".join(name)

        command = Insert('Staging.SatObservationName', '( ObservationName, ObservationId, Source)', name)
        Postgres(command,db)
        value = Observation.split("|")

        del value[:2]
        value =" ".join(value)

        command = Insert('Staging.SatObservationValue', '(ObservationChannel, ObservationValue, ObservationTimeStamp, ObservationId, Source)', "("+value)
        Postgres(command,db)
    else: #dataset2 rest rows
        #Sat
        name = Observation.split("|")
        del name[2:8]
        name =" ".join(name)

        command = Insert('Staging.SatObservationName', '( ObservationName, ObservationId, Source)', name)
        Postgres(command,db)
        value = Observation.split("|")

        del value[:2]
        value =" ".join(value)
        command = Insert('Staging.SatObservationValue', '(ObservationChannel, ObservationValue, ObservationTimeStamp, ObservationId, Source)', "("+value)
        Postgres(command,db)

def attend(ExpHash, GroupHash, SessionHash, Experiment):
    #insert into AttendsSession
    AttendsSessionId = hashlib.sha1((ExpHash+GroupHash+SessionHash).encode()).hexdigest()
    command = Insert('Enterprise.AttendsSession', '(AttendsSessionId, ExperimentalUnitId, GroupId, SessionId, Source)', tuple([AttendsSessionId, ExpHash, GroupHash, SessionHash, Experiment]) )
    Postgres(command,db)    
    
            
# =============================================================================
# start here
# =============================================================================

db = "dbname=smdvault user=smd password=smd2022"

#turncate staging tables
command = staging()
Postgres(command, db)

#insert data by typing
HashKey = TypeData(db)

#extract data from files
path = ["../data/VMData_Blinded", "../PreAutismData_Blinded"] 
Observation = pd.DataFrame({0: None}, index =[0])
countMeta = 0

for p in path: 
    #get in the datasetData_Blinded folder
    os.chdir(p)

    Experiment = p.lstrip('../../')
    UniqueSubject = 0
    sub = ''
    #read all files in the datasetData_Blinded
    for file in os.listdir():
        
        if file.endswith(".csv"): #dataset 1
            print(f'now at {file}')
            FilePath = f".\{file}"
            df = pd.read_csv(FilePath)
            #modify values of metadata
            MetaDataHash = hashlib.sha1(file.encode()).hexdigest()
            
            wavelength = "{"+str(df.iloc[11,1:3].astype('int32').tolist()).strip("[]")+"}"
            StimTime = "{"+str(df.iloc[15,:21].tolist()).replace("'","").strip("[]")+"}"
            df.at[2,'Unnamed: 1'] = df.iloc[2,1].lstrip(' ').rstrip('y')
            df1 = df.iloc[:17,:2]
            #insert subject
            SubjectNow = str(df1.iloc[1,1])
            #insert subject tables when the subject name is different from the last file
            if SubjectNow != sub:
                SubjectHash = hashlib.sha1(SubjectNow.encode()).hexdigest()
                Subject = df1.iloc[1:3,:2].T
                source = ['Source', Experiment]
                Hash = ['SubjectHash', SubjectHash]
                Subject = Subject.assign(source=source)
                Subject = Subject.assign(SubjectHash=Hash)
                Subject.iloc[1,1] = int(Subject.iloc[1,1])
                CountHash = 14
                #insert subject entities
                ParticipatesInId = subject(Subject, SubjectHash, db)
                
                sub = SubjectNow
            #drop subject and unnecessary rows 
            df1 = df1.drop([15,1,2])
            
            #extract metadata
            MetaData = df1.values.tolist()           
            MetaData[9][1] = wavelength
            MetaData[12][1] = StimTime
            for i in range(len(MetaData)):
                try: #change the data type to int
                    MetaData[i][1] = int(MetaData[i][1])
                    t = str(type(MetaData[i][1])).replace("<class '",'').replace("'>",'')
                    MetaData[i].append(t)
                except: #remain str
                    t = str(type(MetaData[i][1])).replace("<class '",'').replace("'>",'')
                    MetaData[i].append(t)
                #HASH metadata here
                MetaData[i].append('VMData_Blinded')
                MetaData[i].append(MetaDataHash)
                MetaData[i] = tuple(MetaData[i])
            
            #insert data once every three file, since one session has three files: Deoxy, Oxy, MES
            if countMeta%3 == 0:
                SessionName = file.split('_H')
                SessionHash = MetaDataHash
                session(MetaDataHash, 'VMData_Blinded', SessionName[0], db)
                ExperimentalUnitIdentifierId = df1.iloc[0,1]
                identifier(ParticipatesInId, 'VMData_Blinded', ExperimentalUnitIdentifierId, db)
                
                attend(SubjectHash, HashKey[CountHash], SessionHash, Experiment)
                
                CountHash +=1
            check = file.replace('_Probe1', '').strip('.csv')
                
            countMeta +=1
            #insert metadata
            meta(MetaData, MetaDataHash, SessionHash, db)
            
            #extract observation data
            df = pd.read_csv(FilePath)
            dft = df.iloc[26:,1:].to_string().split('Unnamed:')
            #check if the time column is not float format
            if ':' in dft[-1]:

                temp = list(df[df['Unnamed: 26'].str.contains(':', na=False)].index)
                
                if len(temp)== 1:
                    df.loc[temp[0],'Unnamed: 26'] = float(df.loc[temp[0]-1,'Unnamed: 26']) + 0.000001
                    record = temp[0]
                elif len(temp)== 0:
                    temp = list(df[df['Unnamed: 50'].str.contains(':', na=False)].index)
                    df.loc[temp[0],'Unnamed: 50'] = float(df.loc[temp[0]-1,'Unnamed: 26']) + 0.000001
                else:
                    df4 = df.iloc[temp,26].str.split(':', 2, expand=True)
                    df.iloc[temp,26] = df4[2]
            #get title for channels
            DataTitle = df.iloc[26, 1:].dropna()
            #get data
            df2 = df.iloc[26:,1:].astype('float32', errors="ignore").dropna(axis=1,how='all').T
            
            df2['value'] = "{" +df2[df2.columns[1:]].apply(lambda x: ','.join(x.dropna().astype(str)),axis=1)+"}"
            Observation = pd.concat([DataTitle,df2['value']], axis=1, ignore_index=True)
            TimeArray = Observation.loc[Observation[0] == 'Time']
            Observation = Observation.drop([TimeArray.index[0]])
            Observation = Observation.rename({0: 'ObservationChannel', 1: 'ObservationValue'}, axis=1)
            Observation['ObservationTimeStamp'] = TimeArray.iloc[0,1]
            ObservationHash = hashlib.sha1(str(file.strip('.csv').replace('_Probe1', '')).encode()).hexdigest()
            Observation['ObservationName'] = check
            Observation['Source'] = 'VMData_Blinded'
            Observation['ObservationHash'] = ObservationHash
            obs(Observation, SessionHash, ObservationHash, MetaDataHash, check, db)
            
            
        else:
            
            folder = f"{p}\{file}"
            #open "Autism000..." folder.
            os.chdir(folder)
            MetaDataHash = hashlib.sha1(folder.encode()).hexdigest()
            #insert subject when the folder has '1_NormalConversation' (That means it is a new subject).
            Subject = re.split("-|\\\\", folder)
            if Subject[2] == '1_NormalConversation':
            
                value = "('"+ str(Subject[1]) + "','" + str(Experiment) + "','" + hashlib.sha1(str(Subject[1]).encode()).hexdigest()  + "')"
                ParticipatesInId = subject(value,  hashlib.sha1(str(Subject[1]).encode()).hexdigest(), db)
                ExperimentalUnitIdentifierId = folder.split('_')
                ExperimentalUnitIdentifierId = ExperimentalUnitIdentifierId[1].lstrip("Blinded\\")
                identifier(ParticipatesInId, 'PreAutismData_Blinded', ExperimentalUnitIdentifierId, db)
                SessionName = file
            else:
                SessionName = file
            
            CountFile = 1
            #open those five files in the folder
            for file in os.listdir():

                f = open(file, errors='ignore')
                #open the third file ".hdr"
                if CountFile == 3:
                    hdr = str(list(f)).replace('\\n', '').replace('\\t', ' ').replace("'","").replace(", ,",",").strip('[]')
                    hdr = re.sub(r', \[\w{4,20}]', '', hdr)
                    hdr = re.split(', |=', hdr)
                    hdr1 = hdr
                    for i in hdr1:
                        count = hdr.index(i)+1
                        keep = hdr.index(i)

                        while i[0] == '"' and i[-1] != '"':
                            
                            i = i +', '+ hdr[count]
                            hdr[count] = hdr[count]+']'
                            hdr.pop(count)
                            
                        hdr[keep] = i.strip('"')
                        if i[-1] == ']':
                            hdr.remove(i)
                        
                    del hdr1
                    
                    hdr = np.array(hdr)
                    r = hdr.shape
                    r = int(r[0])//2
                    hdr = hdr.reshape(r,2).tolist()
                    for i in range(len(hdr)):
                        try:
                            if hdr[i][1] == 'TRUE':
                                hdr[i][1] = True
                            elif hdr[i][1] == 'FALSE':
                                hdr[i][1] = False
                            elif hdr[i][0] == 'StimulusType':
                                hdr[i][1] = ' '
                                
                            hdr[i][1] = eval(hdr[i][1])
                            hdr[i].append(str(type(hdr[i][1])).strip("<class '").strip("'>"))
                        except:
                            hdr[i].append(str(type(hdr[i][1])).strip("<class '").strip("'>").replace('tr', 'str'))
                        hdr[i].append("PreAutismData_Blinded")
                        hdr[i].append(MetaDataHash)
                        hdr[i] = tuple(hdr[i])
                    
                    
                    meta(hdr, MetaDataHash, MetaDataHash, db)
                    
                
                #open the second file ".evt"
                elif CountFile == 2:
                    evt = str(list(f)).replace('\\n', '').replace('\\t', ' ').replace("'","")
                    if evt != "[]":
                        value = "[( 'timeline', '{"+ evt[1:-2] + "}', '" + "list"+"', 'PreAutismData_Blinded', '"+ MetaDataHash +"')]"
                        meta(value, MetaDataHash, MetaDataHash, db)
                    else:
                        #hub
                        command = Insert('Enterprise.HubMetaData', '(MetaDataId, Source)',tuple([MetaDataHash, 'PreAutismData_Blinded']))
                        Postgres(command,db)
                
                        #sat(Session)
                        command = Insert('Enterprise.SatSessionName', '(SessionId, Source, SessionName)', tuple([SessionHash, 'PreAutismData_Blinded', SessionName]))
                        Postgres(command,db)
                        #Link
                        command = Insert('Enterprise.SessionMetaData', '(SessionMetaDataId, SessionId, MetaDataId, Source)', tuple([hashlib.sha1((SessionHash+MetaDataHash).encode()).hexdigest(), SessionHash, MetaDataHash, 'PreAutismData_Blinded']))
                        Postgres(command,db)
                    command = Insert('Enterprise.ObservationMetaData', '(ObservationMetaDataId, Source, ObservationId, MetaDataId)', tuple([hashlib.sha1((ObservationHash+MetaDataHash).encode()).hexdigest(), 'PreAutismData_Blinded', ObservationHash, MetaDataHash]))
                    Postgres(command,db)
                        
                #open the observation data files
                else:
                    ObservationHash = hashlib.sha1(file.encode()).hexdigest()
                    dat = pd.DataFrame(f)
                    dat['split'] = dat[0].str.split(' ')
                    dat = dat.drop([0], axis=1).explode('split').to_numpy()
                    (r,c) = dat.shape

                    #open the first file ".dat"
                    if CountFile == 1:
                        session(MetaDataHash, 'PreAutismData_Blinded', SessionName, db)
                        TimeArray = [i*5.208333 for i in range(r//48)]
                        dat = dat.reshape(48,r//48).astype('float32').tolist()
                        
                        a = 1
                        #one channel has two type: Hbo2 and HbR
                        for i in range(len(dat)):
                            if i%2 ==1:
                                ch = 'HbO2-' +str(a)  
                            else:
                                ch = 'HbR-' + str(a)
                                a +=1
                            value = "( '"+Subject[1]+Subject[2]+"'|,|'"+ch+"'|,|'{"+str(dat[i]).strip("'[]'")+"}'|,|'{"+str(TimeArray).strip("'[]'")+"}'|,|'"+ObservationHash+"'|,|'PreAutismData_Blinded'|)"
                            obs(value, MetaDataHash, ObservationHash, MetaDataHash, i, db)
                    #open the fourth and fifth file
                    else:
                        TimeArray = [i*5.208333 for i in range(r//96)]
                        dat = dat.reshape(96,r//96).astype('float32').tolist()
                        t = 'raw-'+str(CountFile-3)+'-'
                        for i in range(len(dat)):
                            value = "( '"+Subject[1]+Subject[2]+"'|,|'"+t+str(i)+"'|,|'{"+str(dat[i]).strip("'[]'")+"}'|,|'{"+str(TimeArray).strip("'[]'")+"}'|,|'"+ObservationHash+"'|,|'PreAutismData_Blinded'|)"
                            obs(value, MetaDataHash, ObservationHash, MetaDataHash, i, db)
                        command = Insert('Enterprise.ObservationMetaData', '(ObservationMetaDataId, Source, ObservationId, MetaDataId)', tuple([hashlib.sha1((ObservationHash+MetaDataHash).encode()).hexdigest(), 'PreAutismData_Blinded', ObservationHash, MetaDataHash]))
                        Postgres(command,db)
                    
                CountFile +=1
            attend(hashlib.sha1(str(Subject[1]).encode()).hexdigest(), HashKey[CountHash%2+18], MetaDataHash, Experiment)
            CountHash +=1
            print(f'it is now at {folder}')
            #go back to the "..._Blinded" folder
            os.chdir('../') 
            
                
                
            
# =============================================================================
# Rest link 
# =============================================================================
def get(command, db):
  #query data
  conn = None
  try:
      conn = psycopg2.connect(db)
              
      cur = conn.cursor()
      cur.execute(command)
      row = cur.fetchone()
      
      cartisien = []
      while row is not None:
          cartisien.append(row)
          row = cur.fetchone()

      cur.close()
  except (Exception, psycopg2.DatabaseError) as error:
      print(error)
  finally:
      if conn is not None:
          conn.close()
  return cartisien  
   
command = """
            SELECT a.Source, ExperimentalUnitId, GroupId
            FROM Enterprise.HubExperimentalUnit AS a 
            CROSS JOIN Enterprise.HubGroup AS b
            WHERE a.Source = b.Source""" 
assign = get(command, db)

#insert data to AssignedTo
for i in assign:
    t = str(i)
    value = "('"+hashlib.sha1(str(i).encode()).hexdigest()+"', "+ t[1:]
    command = Insert('Enterprise.AssignedTo', '(AssignedToId, Source, ExperimentalUnitId, GroupId)', value)
    Postgres(command,db)
    
# =============================================================================
# load the staging tables to Enterprise layer
# =============================================================================
def strip(i):

    text = str(i).split("'")
    return text[1]

def load(table,db):

    if table == 'SatMetaDataKeyValuePair':
        key = """
                SELECT Key, MIN(MVId)
                FROM Staging.SatMetaDataKeyValuePair
                GROUP BY key
                ORDER BY MIN(MVId)"""
        lst = get(key, db)
        #insert data by rows
        for i in range(31):
            if i < 14 and lst[i][0] != 'Date':
                load = """
                INSERT INTO Enterprise.SatMetaDataKeyValuePair(MetadataId, Source, Key, Value, DataType) 
                SELECT MetadataId, Source, Key, Value, DataType  
                FROM Staging.SatMetaDataKeyValuePair
                """+"WHERE Key = '" + lst[i][0] +"'  OR Key = '"+ lst[-i-1][0] +"'"
                Postgres(load, db)
            elif i < 14 and lst[i][0] == 'Date':
                load = """
                INSERT INTO Enterprise.SatMetaDataKeyValuePair(MetadataId, Source, Key, Value, DataType) 
                SELECT MetadataId, Source, Key, Value, DataType  
                FROM Staging.SatMetaDataKeyValuePair
                """+"WHERE Key = '" + lst[i][0] +"'"
                Postgres(load, db)
                load = """
                INSERT INTO Enterprise.SatMetaDataKeyValuePair(MetadataId, Source, Key, Value, DataType) 
                SELECT MetadataId, Source, Key, Value, DataType  
                FROM Staging.SatMetaDataKeyValuePair
                """+"WHERE Key = '" + lst[-i-1][0] +"'"
                Postgres(load, db)
            else:
                load = """
                INSERT INTO Enterprise.SatMetaDataKeyValuePair(MetadataId, Source, Key, Value, DataType) 
                SELECT MetadataId, Source, Key, Value, DataType  
                FROM Staging.SatMetaDataKeyValuePair
                """+"WHERE Key = '" + lst[i][0] +"'"
                Postgres(load, db)
    
    elif table == 'SatObservationName':
        command = """INSERT INTO Enterprise.SatObservationName(ObservationId, Source, ObservationName)
                        (SELECT DISTINCT ObservationId, Source, ObservationName 
                        	FROM Staging.SatObservationName
                        	)"""
                
        Postgres(command, db)
    else:
        key = """
            SELECT ObservationChannel
            FROM Staging.SatObservationValue
            WHERE ObservationId IN (SELECT DISTINCT MIN(ObservationId)
            						FROM Staging.SatObservationValue
            						GROUP BY ObservationChannel)
            
                """
        lst = get(key, db)
        #insert data by rows
        for i in range(176):
            if i < 24 :
                
                load = """WITH i AS (SELECT ObservationId, timestamp, Source, ObservationValue, ObservationTimeStamp, ObservationChannel
                        		  FROM Staging.SatObservationValue 
                                  WHERE ObservationChannel = '"""+ strip(lst[i]) +"'  OR ObservationChannel = '"+ strip(lst[i+28])+"'  OR ObservationChannel = '"+ strip(lst[i+80])+"'  OR ObservationChannel = '"+ strip(lst[i+176])+"'  OR ObservationChannel = '"+ strip(lst[i+224])+"""'
                        		 )
                        		  
                        INSERT INTO Enterprise.SatObservationValue(ObservationId, Source, ObservationValue, ObservationTimeStamp, ObservationChannel)
                        SELECT i.ObservationId,  i.Source, i.ObservationValue, i.ObservationTimeStamp, i.ObservationChannel
                        FROM i"""
                
                Postgres(load, db)
            elif 23< i <28:
                load = """WITH i AS (SELECT ObservationId, timestamp, Source, ObservationValue, ObservationTimeStamp, ObservationChannel
                        		  FROM Staging.SatObservationValue
                                  WHERE ObservationChannel = '"""+ strip(lst[i]) +"'  OR ObservationChannel = '"+ strip(lst[i+80])+"'  OR ObservationChannel = '"+ strip(lst[i+224]) +"""'
                        		 )
                        		  
                        INSERT INTO Enterprise.SatObservationValue(ObservationId, Source, ObservationValue, ObservationTimeStamp, ObservationChannel)
                        SELECT i.ObservationId,  i.Source, i.ObservationValue, i.ObservationTimeStamp, i.ObservationChannel
                        FROM i"""
                Postgres(load, db)
            elif 51< i < 76:
                load = """WITH i AS (SELECT ObservationId, timestamp, Source, ObservationValue, ObservationTimeStamp, ObservationChannel
                        		  FROM Staging.SatObservationValue 
                                  WHERE ObservationChannel = '"""+ strip(lst[i]) +"'  OR ObservationChannel = '"+ strip(lst[i+56])+"'  OR ObservationChannel = '"+ strip(lst[i+148])+"'  OR ObservationChannel = '"+ strip(lst[i+200])  +"""'
                        		 )
                        		  
                        INSERT INTO Enterprise.SatObservationValue(ObservationId, Source, ObservationValue, ObservationTimeStamp, ObservationChannel)
                        SELECT i.ObservationId,  i.Source, i.ObservationValue, i.ObservationTimeStamp, i.ObservationChannel
                        FROM i"""
                Postgres(load, db)
            elif 131<i<176:
                load = """WITH i AS (SELECT ObservationId, timestamp, Source, ObservationValue, ObservationTimeStamp, ObservationChannel
                        		  FROM Staging.SatObservationValue 
                                  WHERE ObservationChannel = '"""+ strip(lst[i]) +"'  OR ObservationChannel = '"+ strip(lst[i+144]) +"""'
                        		 )
                        		  
                        INSERT INTO Enterprise.SatObservationValue(ObservationId, Source, ObservationValue, ObservationTimeStamp, ObservationChannel)
                        SELECT i.ObservationId,  i.Source, i.ObservationValue, i.ObservationTimeStamp, i.ObservationChannel
                        FROM i"""
                Postgres(load, db)
            else:
                pass
        

load('SatMetaDataKeyValuePair',db)
load('SatObservationName',db)
load('SatObservationValue',db)













   
