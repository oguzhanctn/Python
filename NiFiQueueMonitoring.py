import requests
import sys

print(sys.argv[1]) #NiFi Host as "https://localhost:9090
print(sys.argv[2]) #NiFi ProcessGroup Name on Root with no spaces as "PROCESS_GROUP_NAME"
print(sys.argv[3]) #Max Threshold Size as int "1500"

# Declare Program Variables
nifiProcessGroupsURL=str(sys.argv[1]) + "/nifi-api/process-groups/root/process-groups"
processGroupName_in=str(sys.argv[2])
maxThresHoldSize_in=int(sys.argv[3])
nifiHost = str(sys.argv[1])
slackURL='Slack WebHook URL'

# Get Data From a API URL
def getURL(url):
    r = requests.get(url=url)
    if r.status_code == 200:
        data = r.json()
        return data
    else:
        return r.status_code, r.reason

# API result json to groupListData
groupListData = getURL(nifiProcessGroupsURL)

# Get a NiFi Process Group Connection URL List
def getConList(groupListData):
    conList = []

    for conUrl in groupListData['processGroups']:
        conList.append({"ProcessGroupID": conUrl['status']['id'], "ProcessGroupName": conUrl['status']['name'],
                        "ConnectionListURL": conUrl['uri'] + "/connections"})
    return conList

# Load NiFi Process Group Connection URL List to conURLList
conUrlList = getConList(groupListData)


# Get NiFi Processsor List under a ProcessGroup
def getProcessorURLList(groupListData):
    procList = []

    for procURL in groupListData['processGroups']:
        procList.append({"ProcessorID": procURL['status']['id'], "ProcessGroupName": procURL['status']['name'],
                         "ProcessorURL": procURL['uri'] + "/processors"})
    return procList

# Load Process List URL to procURLList
procURLList = getProcessorURLList(groupListData)

# post to slack message with url and messagebody inputs
def postSlack(url, message):
    p = requests.post(url=url, data=message)
    return p.status_code, p.reason

# Check Queue Threshold Size
def queueThreshold():
    for i in conUrlList:
        conURL = i["ConnectionListURL"]
        conData = getURL(conURL)

        for con in conData['connections']:
            flowFileQueCount = int(con['status']['aggregateSnapshot']['flowFilesQueued'])
            processGroupName = i['ProcessGroupName']
            processGroupID = i['ProcessGroupID']

            if processGroupName == processGroupName_in and flowFileQueCount >= maxThresHoldSize_in:
                messageText = (
                        '*NiFi & ComponentURL :* '+nifiHost + ' & <' + nifiHost + '/nifi/?processGroupId=' + processGroupID + '&componentIds=' +
                        con['id'] + '|GoToNiFiComponent>' + '\n' +
                        '*QueueCount :* ' + str(con['status']['aggregateSnapshot']['flowFilesQueued']) + '\n' +
                        '*QueueSize :* ' + str(con['status']['aggregateSnapshot']['queuedSize']) + '\n' +
                        '*SourceProcessorName :* ' + con['status']['sourceName'] + '\n' +
                        '*DestinationProcessorName :* ' + con['status']['destinationName'])

                messageBody = '{"text":"*NiFi ALERT / ' + processGroupName + '* QueueSize Threshold Warning !!!", "attachments":[{"text":"' + messageText + '","color":"#dc2525",}]} '
                postSlack(slackURL, messageBody)
                print('-----' + processGroupName + '-----' + '\n' + messageText + '\n' + '-----------------------')


queueThreshold()


# Check a Processor Run Status
def processorRunStatus():
    for i in procURLList:
        procURL = i["ProcessorURL"]
        procData = getURL(procURL)

        for proc in procData['processors']:
            processGroupName = i['ProcessGroupName']
            processorID = str(proc['status']['id'])
            processorName = str(proc['status']['name'])
            processorStatus = str(proc['status']['runStatus'])
            if processorStatus != 'Running' and processGroupName == processGroupName_in:
                messageText = (
                        '*NiFi & ProcessorURL :* '+nifiHost + ' & <' + nifiHost + '/nifi/?processGroupId=' + str(proc['status']['groupId']) +
                        '&componentIds=' + proc['id'] + '|GoToNiFiProcessor>' + '\n' +
                        '*ProcessorStatus :* ' + processorStatus + '\n' +
                        '*ProcessorName :* ' + processorName + '\n' +
                        '*ProcessorID :* ' + processorID)

                messageBody = '{"text":"*NiFi ALERT / ' + processGroupName + '-->' + processorName + '* Processor Status Is Not Running, Check the NiFi Flow!! !!!", "attachments":[{"text":"' + messageText + '","color":"#dc2525",}]} '
                postSlack(slackURL, messageBody)
                print('-----'+processGroupName+'-----' + '\n' + messageText + '\n' + '-----------------------')


processorRunStatus()
