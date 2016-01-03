#Version 1.0000.0002
#Version <New Section>.<New Function>.<Function Edits>
#Date 2015-11-15
########################################################################################################################
###############################################                    #####################################################
#############################################  STANDARD SQL PACKAGE  ###################################################
###############################################                    #####################################################
########################################################################################################################

#import cx_Oracle
import pandas as pd
import datetime
import math
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import pymysql
fred51db = pymysql.connect(host='localhost', port=3306, user='root', passwd='Fred5one', db='fred51')
fred51cursor = fred51db.cursor()

#This establishes the database connections for the Fred51 Package
db = fred51db
cursor = fred51cursor

#db = cx_Oracle.connect("SAS_ADHOC_REPORT", "sas_adhoc_report", "dbi00cnc-cphqa:1524/CPHQA")
#cursor = db.cursor()

#Date Created: 2015-11-04
#Last Editted: 2015-11-04
#Author(s): Steven Henkel
#Edited by: Steven Henkel
#Version: 1
#Change Notes:
#Purpose: The purpose of this function is to establish a db connection in the package
#    name <string>: The name of the schema
#    password <string>: The pw for the schema.
#    connection <string>: The connection string for the schema.
#def cxOracleConnection (name, password, connection):
#    global db, cursor
#    db = cx_Oracle.connect(name, password, connection)
#    cursor = db.cursor()

#The purpose of this function is to execute a create table statement, and catch the oracle error if the table already exists
def createTable(sqlCreate):
    name = findNext(sqlCreate,"table")
    errorCheck = 0
    try:
        startDateTime = datetime.datetime.now()
        cursor.execute(sqlCreate)
    except db.Error as e:
        print(str(e))
		
#    except errors.DatabaseError as e:
#        error, = e.args
#        errorCheck = 1
#        #if error.code == 955:
#        #    print('Table Already Exists!')
#    endDateTime = datetime.datetime.now()
#    if (errorCheck == 0):
#        return "Table " + name + " created successfully! Time: " + timeDifference(startDateTime, endDateTime)
#    return "create table " + name + " failed. Error in: " + sqlCreate + " | Error: "+str(error)    

#The purpose of this function is to execute a create index statement, and catch the oracle error if the table already exists
def createIndex(sqlIndex):
    name = findNext(sqlIndex,"index")
    errorCheck = 0
    try:
        startDateTime = datetime.datetime.now()
        cursor.execute(sqlIndex)
    except errors.DatabaseError as e:
        error, = e.args
        errorCheck = 1
        #if error.code == 955:
        #    print('Index Already Exists!')
    endDateTime = datetime.datetime.now()
    if (errorCheck == 0):
        return "Index " + name + " created successfully! Time: " + timeDifference(startDateTime, endDateTime)
    return "create index " + name + " failed Error in: " + sqlIndex + " | Error: "+str(error)    

#The purpose of this function is to execute an alter table statement, and catch the oracle error if the table already exists
def alterTable(sqlAlter):
    name = findNext(sqlAlter,"table")
    errorCheck = 0
    try:
        startDateTime = datetime.datetime.now()
        cursor.execute(sqlAlter)
    except errors.DatabaseError as e:
        error, = e.args
        errorCheck = 1
        #if error.code == 955:
        #    print('Table already exists!')
    endDateTime = datetime.datetime.now()
    if (errorCheck == 0):
        return "Table " + name + " altered successfully! Time: " + timeDifference(startDateTime, endDateTime)
    return "alter table " + name + " failed. Error in: " + sqlAlter + " | Error: "+str(error)  

#The purpose of this function is to execute a drop table statement, and catch the oracle error if the table does not exist           
def dropTable(sqlDrop):
    name = findNext(sqlDrop,"table")
    errorCheck = 0
    try:
        startDateTime = datetime.datetime.now()
        cursor.execute(sqlDrop)
    except:
        print("droptable error")
#    except errors.DatabaseError as e:
#        error, = e.args
#        errorCheck = 1
#        #if error.code == 942:
#        #    print('Table does not exist')
#    endDateTime = datetime.datetime.now()
#    if (errorCheck == 0):
#        return "Table " + name + " dropped successfully! Time: " + timeDifference(startDateTime, endDateTime)
#    return "drop table " + name + " failed. Error in: " + sqlDrop + " | Error: "+str(error)  
            
#The purpose of this function is to execute an insert statement, and catch the oracle error if the table does not exist to insert into
def insertTable(sqlInsert):
    name = findNext(sqlInsert,"into")
    errorCheck = 0
    try:
        startDateTime = datetime.datetime.now()
        cursor.execute(sqlInsert)
        cursor.execute("commit")
    except db.Error as e:
        print(str(e))
#    except errors.DatabaseError as e:
#        error, = e.args
#        errorCheck = 1
#        #if error.code == 942:
#        #    print('Table does not exist!')
#    endDateTime = datetime.datetime.now()
#    if (errorCheck == 0):
#        return "Table " + name + " inserted successfully! Time: " + timeDifference(startDateTime, endDateTime)
#    return "insert into " + name + " failed. Error in: " + sqlInsert + " | Error: "+str(error) 

#The purpose of this function is to execute an update statement, and catch the oracle error if the table does not exist to update into
def updateTable(sqlUpdate):
    name = findNext(sqlUpdate,"update")
    errorCheck = 0
    try:
        startDateTime = datetime.datetime.now()
        cursor.execute(sqlUpdate)
        cursor.execute("commit")
    except errors.DatabaseError as e:
        error, = e.args
        errorCheck = 1
        #if error.code == 942:
        #    print('Table does not exist!')
    endDateTime = datetime.datetime.now()
    if (errorCheck == 0):
        return "Table " + name + " updated successfully! Time: " + timeDifference(startDateTime, endDateTime)
    return "update table " + name + " failed. Error in: " + sqlUpdate + " | Error: "+str(error) 

#This function is required in the looping procedure to replace SQL cursor variables with values
#    sqlQuery: The sql statment with the SQL cursor variables
#    prefix: What the variable is prefixed with (Typically "i.")
#    suffix: What is the variable suffixed with (Typically nothing, but this allows for additional capabilities for the function)
#    replaceTuple: a 2 dimensional tuple where the first element is what you want to replace (with the prefix a/d suffix applied) and the second element is what will replace it.
#    stringQuotes: Set equal to "Y" if you want to replace the variable with the second element of the tuple with single quotes so that it works with sql statments
def replaceVariables(sqlQuery, prefix, suffix, replaceTuple,stringQuotes,overrideQuotes):
    tempQuery = sqlQuery
    for i in range(0,len(replaceTuple)):
        if isDateTime(str(replaceTuple[i][1])):
            tempQuery = tempQuery.replace(prefix+replaceTuple[i][0]+suffix,dateStringSQL(replaceTuple[i][1]))
        elif not(isNumber(str(replaceTuple[i][1]))) and stringQuotes == "Y" or overrideQuotes==True:
            tempQuery = tempQuery.replace(prefix+replaceTuple[i][0]+suffix,"'"+replaceTuple[i][1]+"'")
        tempQuery = tempQuery.replace(prefix+replaceTuple[i][0]+suffix,replaceTuple[i][1])
    return tempQuery

#The purpose of this function is to determine if a value is datetime or not. Only works for basic cases.
def isDateTime(value):
    if str(value).find("datetime64[ns]") >=0 or str(value).find("00:00:00") >0:
        return True
    else:
        return False

#This function checks to see if the string value is actually a number eg isNumber('1') = true, isNumber('s') = false 
def isNumber(value):
    try:
        int(value)
        return True
    except ValueError:
        return False

#The purpose of this function is to make a timestamp into a sql to_date function.
def dateStringSQL(timeStamp):
    dateStringBase =str(timeStamp)
    dateStringClone=""
    dateStringClone = str(timeStamp).replace("0","#").replace("1","#").replace("2","#").replace("3","#").replace("4","#").replace("5","#").replace("6","#").replace("7","#").replace("8","#").replace("9","#")
    datePosition = dateStringClone.find("####-##-##")
    return "to_date('" +  dateStringBase[datePosition:datePosition+10] + "','YYYY-MM-DD')"
    
#This is the function that loop inserts code into a table.
#    loopCursor: The cursor table in the sql loop
#    cursorTuple: A tuple that contains all of the column names that need values to be replaced in the loop
#    prefix: What the variable is prefixed with (Typically "i.")
#    suffix: What is the variable suffixed with (Typically nothing, but this allows for additional capabilities for the function)
#    insertSQL: The insert statement that is to be looped
def insertTableLoop(loopCursor, cursorTuple, prefix, suffix, insertSQL,overrideQuotes):
    errorCheck = 0
    try:
        startDateTime = datetime.datetime.now()
        df = pd.read_sql(loopCursor,db)
        #The purpose of this loop is to create the sql statement for each iteration and run the insert statement
        for i in range(0,int(str(df.count()).split()[1])):#Count of elements in the cursor
            tempQuery = insertSQL #The tempQuery will be reset to insertSQL at the beginning of the [i] for loop.
            #The purpose of this loop is to iteratively replace the columns with the variable values
            for j in range(0,len(cursorTuple)): # goes from 0 to 1 if there is a tuple length 1
                tempQuery = replaceVariables(tempQuery,prefix,suffix,((cursorTuple[j],str(df[cursorTuple[j]][i])),),"Y",overrideQuotes)    
            cursor.execute(tempQuery)
            cursor.execute("commit")
    except cx_Oracle.DatabaseError as e:
        errorCheck = 1
        error, = e.args
        #if error.code == 942:
        #    print('Table does not exist!')
        #if error.code == 979:
        #    print('Not a Group by Expression!')
        #else:
        #    print("Loop Table failed. Error in: " + insertSQL + " | Error: "+str(error)  + str(error.code))
    endDateTime = datetime.datetime.now()
    if (errorCheck == 0):
        return " SQL loop completed successfully! Time: " + timeDifference(startDateTime, endDateTime)
    return " SQL loop failed. Error in: " + insertSQL + " | Error: "+str(error) 
            
#This function was only created for testing purposes.
def insertTableLoopLog(loopCursor, cursorTuple, prefix, suffix, insertSQL):
    try:
        df = pd.read_sql(loopCursor,db)
        log = ""
        #The purpose of this loop is to create the sql statement for each iteration and run the insert statement
        for i in range(0,int(str(df.count()).split()[1])):#Count of elements in the cursor
            tempQuery = insertSQL #The tempQuery will be reset to insertSQL at the beginning of the [i] for loop.
            #The purpose of this loop is to iteratively replace the columns with the variable values
            for j in range(0,len(cursorTuple)): # goes from 0 to 1 if there is a tuple length 1
                tempQuery = replaceVariables(tempQuery,prefix,suffix,((cursorTuple[j],str(df[cursorTuple[j]][i])),),"N")    
            log=log + "i=" + str(i) + "j=" + str(j) + " " + tempQuery
        return log
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        if error.code == 942:
            print('Table does not exist!')

#The purpose of this function is to convert a text string into a valid name for SQL (30 characters, no punctuation")
def convertSQLNames (columnEntry):
    columnEntry=columnEntry.replace(" ","_").replace(":","_").replace(";","_").replace("'","").replace(",","_").replace("?","_").replace("\\","_").replace("(","_").replace(")","_").replace("/","_").replace("[","_").replace("]","_").replace("#","_").replace(".","_").replace("$","_").replace("%","_").replace("-","_").replace("!","_").replace("&","_")
    columnEntry=columnEntry.replace("__________","_").replace("_________","_").replace("________","_").replace("_______","_").replace("______","_").replace("_____","_").replace("____","_").replace("___","_").replace("__","_")
    columnEntry=columnEntry[:25]
    return columnEntry

#The purpose of this function is to create a transposed table from an existing table, transposing one element.
#    sourceTable: The table you want to transpose data from
#    newColumnNames: The column you want to transpose and make into new column names
#    transposedValues: If you leave this as an empty string "" then it will make the columns dummy's (0,1)
#                      If you put a column name from the table here, it will transpose those values
#    defaultPrefix: put True if you want to use an automatic prefix for the column names to ensure distinct. Otherwise no prefix (might crash if columns are not distinct!)
def transposeSQL (sourceTable, newColumnNames, transposedValues, transposedTableName, defaultPrefix):
    dfColumns = pd.read_sql("select column_name from USER_TAB_COLUMNS where table_name = '"+sourceTable+"'",db)
    dfDistinct = pd.read_sql("select distinct "+newColumnNames+" as CATEGORY from "+sourceTable,db)
    transposeSQL = "create table "+transposedTableName+" as select "
    transposeGroup = "group by "
    for i in range(0,dfColumns['COLUMN_NAME'].count()): #Goes from the first column to the last column in the table
        if dfColumns['COLUMN_NAME'][i] != newColumnNames and dfColumns['COLUMN_NAME'][i] != transposedValues:
            transposeSQL=transposeSQL+dfColumns['COLUMN_NAME'][i]+", "
            if i != dfColumns['COLUMN_NAME'].count()-1:
                transposeGroup=transposeGroup + dfColumns['COLUMN_NAME'][i]+", "
            else:
                transposeGroup=transposeGroup + dfColumns['COLUMN_NAME'][i]
    transposeCaseWhen = ""
    transposedValuesEnd = ""
    if transposedValues == "":
        transposeValuesEnd = "1 else 0 end"
    else:
        transposeValuesEnd = transposedValues + " end"
    transposePrefix = ""
    for i in range(0,dfDistinct['CATEGORY'].count()):
        
        if defaultPrefix:
            transposePrefix = "c" + str(i) + "_"
        else:
            if isNumber(convertSQLNames(dfDistinct['CATEGORY'][i])[0]):
                transposePrefix = "NXQ_"
            else:
                transposePrefix = ""
        if i != dfDistinct['CATEGORY'].count()-1:
            transposeCaseWhen = transposeCaseWhen + "max(case when "+newColumnNames+" = '" + dfDistinct['CATEGORY'][i].replace("'","''").replace("&","' || chr(38) || '") + "' then " + str(transposeValuesEnd) + ") as " + transposePrefix + convertSQLNames(dfDistinct['CATEGORY'][i])[:30] + ", "
        else:
            transposeCaseWhen = transposeCaseWhen + "max(case when "+newColumnNames+" = '" + dfDistinct['CATEGORY'][i].replace("'","''").replace("&","' || chr(38) || '") + "' then " + str(transposeValuesEnd) + ") as " + transposePrefix + convertSQLNames(dfDistinct['CATEGORY'][i])[:30]
    dropTable("drop table "+transposedTableName)
    createTable(transposeSQL + " " + transposeCaseWhen + " from " + sourceTable + " " + transposeGroup)
    #return transposeSQL + " " + transposeCaseWhen + " from " + sourceTable + " " + transposeGroup

#The purpose of this function is to transpose dummy columns from a table into 1 column
#    groupList: The columns in the table you do not wish to transpose (the non-dummy columns)
#    sourceTableName: The table you want to transpose data from
#    newTableName: The name of the new table you are transposing
#    newColumnName: The name of the column you would like to transpose the dummy columns into
#    newColumnNameFormat: The format you would like the new column to have
#    Prefix: Put "" if you don't want to specify a prefix to remove if found. Otherwise the function will remove the prefix for you. 
def transposeReverseSQL (groupList, sourceTableName, newTableName, newColumnName, newColumnNameFormat, prefix):
    df = pd.read_sql("select * from "+str(sourceTableName),db)
    groupBase = ""
    for i in range(0,len(groupList)):
        groupBase = groupBase + groupList[i]
        if len(listtest)-1 > i:
            groupBase = groupBase + ","
    baseTableSql = "create table " + str(newTableName) + " as select " + groupBase + ", cast('' as "+newColumnNameFormat+") " + " as " + newColumnName + " from " + sourceTableName + " where 1 = 0"        
    dropTable("drop table " + newTableName)
    createTable (baseTableSql)
    #return baseTableSql
    for i in range(0,len(list(df.columns.values))):
        columnName = df.columns[i]
        if(columnName[:4] == "NXQ_"):
            columnName = columnName[4:]
        elif(prefix != "" and columnName[:len(prefix)] == prefix):
            columnName = columnName[len(prefix):]
        if(not(df.columns[i] in groupList)):
            insertIteration = "insert /*+append*/ into " +newTableName + "(" + groupBase + "," + newColumnName + ") (select " + groupBase + ", " + "cast('" + columnName + "' as " + newColumnNameFormat + ") as " + newColumnName + " from " + sourceTableName + " where " + df.columns[i] + " = 1)"
            insertTable(insertIteration)
    #return insertIteration
    #return range(0,len(list(df.columns.values)))
    
######################################################################################################################
###############################################                  #####################################################
#############################################  SQL PARSER PACKAGE  ###################################################
###############################################                  #####################################################
######################################################################################################################

import re

#The purpose of this function is to find the nth element of a search string in a given string. This is very similar to the Excel find function.
#    fullText: The given text field we are looking to find a substring in.
#    findText: The text we are looking to find within the fullText
#    n: The iteration we are looking for. The first iteration starts at 0
def findIteration(fullText, findText, n):
    parts= fullText.split(findText, n+1)
    if len(parts)<=n+1:
        return -1
    return len(fullText)-len(parts[-1])-len(findText)

#Date Created: 2015-10-26
#Last Editted: 2015-10-26
#Author(s): Steven Henkel
#Edited by: Steven Henkel
#Purpose: The purpose of this function is to find if a string exists after another string in a given base string, 
#and return the start or end position.
#    baseString <String>: The text field we are searching in.
#    firstString <String>: The first string we want to use as the starting point
#    secondString <String>: The string we want to find after the first string
#    separation <Integer>: The maximum number of words the next string can be away (defined by spaces)
#    exact <Boolean>: True if we want the second string to be exactly the number of words away defined in seperation
#    start <Boolean>: True if we want to return the start of the second string location as opposed to the end 
def findNear (baseString,firstString,secondString,separation,exact,start):
    secondStringLength = len(secondString)+1
    baseStringClean = baseString.lower()
    firstStringClean = firstString.lower()
    secondStringClean = secondString.lower()
    baseStringParts= baseStringClean.split(" ", baseStringClean.count(" "))
    noResultCheck = 0
    if(start == True):
        secondStringLength = 0
    for i in range(0,len(baseStringParts)-1):
        if(baseStringParts[i] == firstStringClean):
            if(exact==True):
                if(baseStringParts[i+separation] == secondStringClean):
                    noResultCheck=1
                    return findIteration(baseStringClean,firstStringClean,0) + findIteration(" ".join(baseStringParts[i:])," "+secondString+" ",0)+1 + secondStringLength
                else:
                    noResultCheck=1
                    return -1
            else:
                for j in range(i+1,min(len(baseStringParts)-1-i,separation+i+1)):
                    if(baseStringParts[j] == secondStringClean):
                        noResultCheck=1
                        return findIteration(baseStringClean,firstStringClean,0) + findIteration(" ".join(baseStringParts[i:])," "+secondString+" ",0)+1 + secondStringLength
    if(noResultCheck == 0):
        return -1

#Date Created: 2015-10-27
#Last Editted: 2015-10-27
#Author(s): Steven Henkel
#Edited by: Steven Henkel
#Purpose: The purpose of this function is to find the first time a stirng is found not encapsulated by other strings
#    baseString <String>: The text field we are searching in.
#    subString <String>: the text we want to find not encapsulated by the encapsulatedList
#    encapsulatedList <2D Tuple String>: a list of corresponding encapsulations (eg. [],/* */,' ') that you want to ignore
#        when searching for the subString
### Example
### baseString = "select ';' * from /*;*/ george; Yippee!"
### subString = ";"
### encapsulatedList = (("/*","*/"),('"','"'),("'","'"))
def findFree (baseString, subString, encapsulatedList):
    if(baseString.count(subString) !=0):
        changeString = baseString
        fullList = encapsulatedList + ((subString,"NXQCorrect"),)
        counter = 0
        charCount = 0
        isFound = 0
        #log = "Start: "
        #log += " baseString: [" + baseString + "] Length: " + str(len(baseString))
        while (charCount <= len(baseString)):
            #log += " Iteration: " + str(counter)
            firstString = findFirst(changeString,[i[0] for i in fullList])
            #log += " first String: " + firstString
            firstPosition = findIteration(changeString,firstString,0)
            #log += ", " + str(firstPosition)
            correspondingString = fullList[[i for i, v in enumerate(fullList) if v[0] == firstString][0]][1]
            #log += " Corresponding String: " + correspondingString
            correspondingPosition = findIteration(changeString[firstPosition+len(firstString):],correspondingString,0)
            #log += ", " + str(correspondingPosition)
            if(correspondingString == "NXQCorrect"):
                isFound = 1
                #log += " Finished and Found! Final Answer: " + str(charCount + firstPosition)
                return charCount + firstPosition
                break
            else:
                counter+=1
                charCount += firstPosition + correspondingPosition+len(firstString) + len(correspondingString)
                changeString = changeString[firstPosition + correspondingPosition+len(firstString) + len(correspondingString):]
                #log += " Not Found, new charCount:  " + str(charCount) + " changeString: [" + changeString + "] Looking again."
            if(counter>=100000):
                #log += " Too many Iterations!"
                break
        if(isFound == 0):
            #log += " None Found :("
            return -1
    else:
        return -1

#Date Created: 2015-09-04
#Last Editted: 2015-11-02
#Author(s): Steven Henkel
#Edited by: Steven Henkel
#Version: 2
#Change Notes:
#     2015-11-02: Fixed issue when only spaces in text, causing text[-1] to cause an error. Also add tabs as something to compress (V1 - V2)
#Purpose: The purpose of this function is to remove all the spaces from a string.
#    text <string>: The text to have the spaces removed.
def spaceCompress(text):
    count=0
    text=text.replace("\n"," ").replace("\t"," ")
    if(len(text)>0):
        while (count <10):
            text = text.replace("  "," ")
            count +=1
        if (text[0] == " "):
            text = text[1:]
        if (len(text) > 0):
            if (text[-1] == " "):
                text = text[:-1]
    return text

#The purpose of this function is to find from a list of strings, which is the first string to show up in a given text field.
#    fullText: The given text field we are looking to find a the first iteration of a substring in.
#    findTextList: This is a list that contains all the substrings we are looking for.
def findFirst(fullText,findTextList):
    #minList = []
    minPosition = 10000000000000000000000000000000000
    minValue = "None Found"
    for i in findTextList:
        foundPosition = findIteration(fullText, i, 0)
        if (foundPosition != -1):
            if(foundPosition < minPosition or findIteration(i, minValue, 0) != -1 and foundPosition == minPosition):
                minPosition = foundPosition
                minValue = i
    return minValue            

#Date Created: 2015-09-04
#Last Editted: 2015-11-02
#Author(s): Steven Henkel
#Edited by: Steven Henkel
#Version: 2.01
#Change Notes:
#     2015-10-28: Ignore semicolons in text. Allow specialized parsing of sql loops (V1 - V2)
#     2015-11-02: Changed findPrevious from 1 to 0, added spaceCompress to resulting sub (V2 - V2.01)
#Purpose: The purpose of this function is to parse a sql query into its constituent parts so that the full sql script can
#be run without having to split each query into it's own function. this is very handy for simple but long queries
#to reduce the time spent parsing the query.
#    scriptOrig: This is the original script passed into the sqlParser to be parsed.
def sqlParser (scriptOrig):
    script = spaceCompress(commentCleaner(scriptOrig))
    sqlTuple = [["",""],]
    check="T"
    #log=""
    findPrevious = 0
    findCurrent = findFree(script,";",(("/*","*/"),('"','"'),("'","'"),("declare"," end loop; end")))
    #log = "Start: " + str(findCurrent)
    while (findCurrent != -1):
        sub = script[findPrevious:findCurrent]
        #log += " sub: [" + sub + "]"
        match = re.search("[A-Z]{1,100} [A-Z]{1,100}",sub.upper())
        if(not(match is None)):
            sqlTuple += [[match.group(),spaceCompress(sub)],]
        #log += " tuple name: " + match.group()
        script = script[findCurrent+1:]
        #log += " new script: " + script
        findPrevious = 0
        findCurrent = findFree(script,";",(("/*","*/"),('"','"'),("'","'"),("declare"," end loop; end")))
    return sqlTuple[1:] 

#The purpose of this function is to be able to clean out any commented code from SQL so the parser can work correctly.
#    script: The script to have the comments cleaned out of.
def commentCleaner(script):
    cleanedScript = ""
    remainderScript = script
    fullLog = ""
    countBreak = 0
    #check = 0
    while((findIteration(remainderScript,"/*",0) >=0 or findIteration(remainderScript,"--",0) >=0 or findIteration(remainderScript,"'",0) >=0) and countBreak < 1000):
        condition = findFirst(remainderScript,["/*+","/*","--","'"])
        if (condition == "/*+"):
            cleanedScript = cleanedScript + spaceCompress(remainderScript[:findIteration(remainderScript,"*/",0)+2])+" "
            remainderScript = remainderScript[findIteration(remainderScript,"*/",0)+2:]
            #check=1
        elif (condition == "/*"):
            cleanedScript = cleanedScript + spaceCompress(remainderScript[:findIteration(remainderScript,"/*",0)])+" "
            remainderScript = remainderScript[findIteration(remainderScript,"/*",0)+findIteration(remainderScript[findIteration(remainderScript,"/*",0):],"*/",0)+2:]
            #check=2                      
        elif (condition == "--"): 
            cleanedScript = cleanedScript + spaceCompress(remainderScript[:findIteration(remainderScript,"--",0)])+" "
            remainderScript = remainderScript[findIteration(remainderScript,"--",0) + findIteration(remainderScript[findIteration(remainderScript,"--",0):],"\n",0)+1:]
            #check=3
        elif (condition == "'"): 
            cleanedScript = cleanedScript + remainderScript[:findIteration(remainderScript,"'",1)+2]
            remainderScript = remainderScript[findIteration(remainderScript,"'",1)+2:]   
        countBreak += 1
    cleanedScript += " " + remainderScript
    if countBreak == 1000:
        return "Infinite Loop!"
    else:
        return cleanedScript

#Date Created: 2015-10-23
#Last Editted: 2015-10-29
#Author(s): Steven Henkel
#Edited by: Steven Henkel
#Version: 1.1
#Change Notes:
#     2015-10-29: fixed bug when name was at the end it would truncate the last letter
#Purpose: The purpose of this function is to return the name of the object, based on text expected to be found right before 
#         the name in a SQL statement
#Parameters:
#    baseSql: The SQL script you are trying to find the name from
#    priorString: the string expected to be found preceeding the name being searched for
def findNext(baseSql,priorString):
    name = spaceCompress(commentCleaner(baseSql.lower()).replace("("," "))
    name = name[findIteration(name,priorString.lower(),0)+len(priorString)+1:]
    if(findIteration(name," ",0)>=0):
        name = name[:findIteration(name," ",0)]
    return(name)

#This is a synonym to findNext. Will eventually be deprecated
def returnSqlName(baseSql,priorString):
     findNext(baseSql,priorString)

#Date Created: 2015-09-10
#Last Editted: 2015-11-02
#Author(s): Steven Henkel
#Edited by: Steven Henkel
#Version: 2.02
#Change Notes:
#     2015-10-28: Allow parsing and running of SQL Loops (V1 - V2)
#     2015-10-29: enables logging (V2 - V2.01)
#     2015-11-29: break if oracle error (V2.01-V2.02)
#Purpose: The purpose of this function is to run the sql statements from the provided sql script. This is the first function
#          that needs to be called, the rest of the code from this package are helper functions.
#Depandancies: sqlParser, commentCleaner, findFirst, findIteration, spaceCompress, functions from "STANDARD SQL PACKAGE"
#Parameters:
#    sqlText: The text that will be parsed and run. So far only works for:
#             CREATE TABLE 
#             DROP TABLE
#             ALTER TABLE 
#             INSERT INTO
#             CREATE INDEX
#             DECLARE CURSOR
#             will also try to run other types, without success guarantee
def sqlRunner (sqlText):
    scriptList = sqlParser(sqlText)
    startDateTime = datetime.datetime.now()
    log="sqlRunner: Start Log: " + str(startDateTime)[:19] + "\n"
    iterationLog = log
    print(iterationLog)
    for i in scriptList:
        if (i[0] == "DROP TABLE"):
            iterationLog = dropTable(i[1])
        elif (i[0] == "CREATE TABLE"):
            iterationLog = createTable(i[1])
        elif (i[0] == "INSERT INTO" or "INTO" in i[0] or "APPEND PARALLEL" in i[0]): #May need to be revised due to issue with /*+append*/
            iterationLog = insertTable(i[1])
        elif (i[0] == "ALTER TABLE"):
            iterationLog = alterTable(i[1])
        elif (i[0] == "CREATE INDEX"):
            iterationLog = createIndex(i[1])
        elif (i[0] == "DECLARE CURSOR"):
            cleanLoopSql = spaceCompress(commentCleaner(i[1]))
            startCursor = findNear(cleanLoopSql.lower(),"cursor","is",2,True,False)
            endCursor = startCursor + findFree(cleanLoopSql[startCursor:],";",(("/*","*/"),('"','"'),("'","'")))
            finalLoopCursor = cleanLoopSql[startCursor:endCursor]
            dfLoop = pd.read_sql(finalLoopCursor,db)
            finalCursorTuple = list(dfLoop.columns.values)
            finalLoopPrefix = "i."
            finalLoopSuffix = ""
            startInsert = findIteration(cleanLoopSql[endCursor:].lower(),"insert",0)
            endInsert = startInsert + findFree(cleanLoopSql[endCursor + startInsert:],";",(("/*","*/"),('"','"'),("'","'")))
            finalLoopInsert = cleanLoopSql[endCursor:][startInsert:endInsert]
            iterationLog = insertTableLoop(finalLoopCursor, finalCursorTuple, finalLoopPrefix, finalLoopSuffix, finalLoopInsert,False) 
        else:
            errorCheck=0
            try:
                defaultStartDateTime = datetime.datetime.now() 
                cursor.execute(i[1])
            except cx_Oracle.DatabaseError as e:
                error, = e.args
                errorCheck=1
                #log+= " " + str(error.code)
            defaultEndDateTime = datetime.datetime.now() 
            if (errorCheck == 0):
                iterationLog = i[0] + " completed successfully! Time: " + timeDifference(defaultStartDateTime, defaultEndDateTime)
            else:
                iterationLog = i[0] + " failed. Error in: " + i[1] + " | Error: "+str(error)
        log += " " + str(iterationLog) + "\n"
        print(iterationLog)
        if(not(findIteration(str(iterationLog),"Error: ORA",0)==-1) and findIteration(str(iterationLog), "ORA-00942",0) == -1):
            break
    endDateTime = datetime.datetime.now()  
    log += " End: " + str(endDateTime)[:19] + "\n Full Run Time: " + timeDifference(startDateTime, endDateTime) + "\n"
    return log

#Date Created: 2015-10-28
#Last Editted: 2015-10-28
#Author(s): Steven Henkel
#Edited by: Steven Henkel
#Version: 1
#Purpose: The purpose of this function to to pad zeroes to a given number of digits
#    number: The number to be padded
#    digits: The number of digits to pad
def zeroStrings (number, digits):
    zeroes = ""
    for i in range(0,digits-1):
        zeroes += "0"
    return (zeroes + str(number))[-digits:]

#Date Created: 2015-10-28
#Last Editted: 2015-10-28
#Author(s): Steven Henkel
#Edited by: Steven Henkel
#Version: 1
#Purpose: The purpose of this function is to return the amount of time between to datetime variables
#    startDateTime: The first date to be compared
#    endDateTime: The second date to be compared
def timeDifference (startDateTime, endDateTime):
    startDateNumber = startDateTime.second + startDateTime.minute * 60 + startDateTime.hour * 60*60 + startDateTime.day*60*60*24
    endDateNumber = endDateTime.second + endDateTime.minute * 60 + endDateTime.hour * 60*60 + endDateTime.day*60*60*24
    differenceNumber = endDateNumber - startDateNumber
    differenceString = ""
    if(not(math.floor(differenceNumber/(60*60*24))==0)):
        differenceString += zeroStrings(math.floor(differenceNumber/(60*60*24)),3) + ":"
    #if(not(math.floor(differenceNumber/(60*60))==0)):
    #    differenceString += zeroStrings(math.floor(differenceNumber/(60*60))%(60*60),2)  + ":"
    #if(not(math.floor(differenceNumber/(60))==0)):
    #    differenceString += zeroStrings(math.floor(differenceNumber/(60))%60,2)  + ":"
    #differenceString += zeroStrings(differenceNumber%60,2)
    differenceString += zeroStrings(math.floor(differenceNumber/(60*60))%(60*60),2)  + ":"
    differenceString += zeroStrings(math.floor(differenceNumber/(60))%60,2)  + ":"
    differenceString += zeroStrings(differenceNumber%60,2)
    differenceString += " (seconds: " + str(differenceNumber) + ")"
    return spaceCompress(differenceString)

#Date Created: 2015-10-29
#Last Editted: 2015-11-03
#Author(s): Steven Henkel
#Edited by: Steven Henkel
#Version: 2
#Change Notes:
#     2015-11-03: Add str to variables to reduce crashing (V1 - V2)
#Purpose: The purpose of this function is to save the log to the directory of the .py file. 
#         It will append if the file already exists
#Parameters:    
#    log <string>: The content to be added to the log
#    name <string>: The name of the log file
def logHandler (log, name):
    logFile = open(str(name)+".txt","a")
    logFile.write(str(log) + "\n")
    logFile.close()

#Date Created: 2015-10-29
#Last Editted: 2015-10-29
#Author(s): Steven Henkel
#Edited by: Steven Henkel
#Version: 1
#Purpose: The purpose of this function is to be able to send emails with content. Attachement Options not yet available.
#Parameters:    
#    emailFrom <string>: Who the email is from
#    emailTo <string>: Who the email is sent to
#    emailHost <string>: The email host being used ("mail.rim.net" at BB)
#    emailSubject <string>: The subject of the email
#    emailContent <string>: The content of the email
#Example: emailHandler("shenkel@blackberry.com","shenkel@blackberry.com", "mail.rim.net","Quartz Dataload Log",log)
def emailHandler (emailFrom, emailTo, emailHost, emailSubject, emailContent):
    msg = MIMEMultipart('alternative')
    msg['Subject'] = emailSubject
    msg['From'] = emailFrom
    msg['To'] = emailTo
    bodyHtml ="""<html>
    <head></head>
    <body>
    <p>""" + emailContent.replace("\n","<br>") + """</p>
    </body>
    </html>"""
    part = MIMEText(bodyHtml, 'html')
    msg.attach(part)
    s = smtplib.SMTP(emailHost)
    s.sendmail(emailFrom, emailTo, msg.as_string())
    s.quit()

