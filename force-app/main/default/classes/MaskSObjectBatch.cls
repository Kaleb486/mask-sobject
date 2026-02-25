/**
 * @description This class is used to mask the database by performing records updates
 * Modified to support high-entropy combinatorics generation and custom formats.
 */
public with sharing class MaskSObjectBatch implements Database.Batchable<sObject>{

    // --- CUSTOM: Wrapper for decoupled dictionary arrays ---
    public class CombinatoricsDict {
        public List<String> firstNames;
        public List<String> lastNames;
        public List<String> domains;
    }
    private CombinatoricsDict dictionary;

    private Integer sequence;
    private Integer lastSequence;
    private Integer batchSize;

    @TestVisible
    private String sobjectName;
    private Boolean allowExecutionOnProd = Test.isRunningTest() ? true : MaskSObjectUtils.getOptions().AllowExecutionOnProd__c;

    private List<MaskSObjectField__c> fields;
    private List<String> queryFields;
    private String whereClause;
    private Map<String, Schema.DisplayType> fieldType;
    public String mode = MaskSObjectConstants.MODE_ONESHOT;

    public MaskSObjectBatch(Integer sequence){
        setParams(sequence, '%');
    }

    public MaskSObjectBatch() {
        setParams(null, '%');
    }

    public MaskSObjectBatch(Boolean isScheduledMode){
        if(isScheduledMode){
            this.mode = MaskSObjectConstants.MODE_SCHEDULED;
        }
        setParams(null, '%');
    }

    public MaskSObjectBatch(String sObjectName){
        setParams(sequence, sObjectName);
    }

    public Database.QueryLocator start(Database.BatchableContext BC){
        String query;

        AsyncApexJob job = getJob(BC.getJobId());
        try {
            upsert new MaskSObject__c(APIName__c = this.sobjectName, LastStart__c = System.now(), LastEnd__c = null,
            LastJobId__c = job.Id, JobItemsProcessed__c = job.JobItemsProcessed, TotalJobItems__c = job.TotalJobItems,
            NumberOfErrors__c = job.NumberOfErrors) APIName__c;

            query = 'SELECT Id, ' + String.join(this.queryFields, ',') + ' FROM ' + String.escapeSingleQuotes(this.sobjectName);

            if(!MaskSObjectUtils.isSandbox() && !allowExecutionOnProd){
                query += ' WHERE Id=null';
                System.debug(LoggingLevel.WARN, 'MaskSObjectBatch execution on production is forbidden, query updated to return 0 rows');
            } else if(String.isNotBlank(whereClause)){
                query += ' WHERE ' + whereClause;
            }
            return Database.getQueryLocator(query);
        } catch (Exception e) {
            MaskSObjectUtils.saveError(e, job.Id, this.sobjectName);
            return Database.getQueryLocator(query);
        }
    }

    public void execute(Database.BatchableContext BC, List<SObject> scope){
        AsyncApexJob job;
        try {
            job = getJob(BC.getJobId());

            for(SObject sobj : scope){
                
                // --- CUSTOM: Dynamically build the dictionary model per record ---
                MaskSObjectDictionaryModel data = new MaskSObjectDictionaryModel();
                
                if (dictionary != null && dictionary.firstNames != null) {
                    Integer fIndex = Math.mod(Math.abs(Crypto.getRandomInteger()), dictionary.firstNames.size());
                    Integer lIndex = Math.mod(Math.abs(Crypto.getRandomInteger()), dictionary.lastNames.size());
                    Integer dIndex = Math.mod(Math.abs(Crypto.getRandomInteger()), dictionary.domains.size());

                    String fName = dictionary.firstNames[fIndex];
                    String lName = dictionary.lastNames[lIndex];
                    String domain = dictionary.domains[dIndex];

                    data.Firstname = fName;
                    data.Lastname = lName;
                    data.Fullname = fName + ' ' + lName;
                    data.Email = fName.toLowerCase() + '.' + lName.toLowerCase() + '@' + domain;
                }

                for(MaskSObjectField__c field : this.fields){
                    if(this.mode.equals(field.Mode__c) || MaskSObjectConstants.MODE_BOTH.equals(field.Mode__c)){
                        
                        // --- CUSTOM: Interceptors for Phone and Alphanumeric IDs ---
                        if (field.ActionType__c == 'Random_US_Phone') {
                            sobj.put(field.APIName__c, generateFakePhone());
                        } 
                        else if (field.ActionType__c == 'Alphanumeric_Preserve_Length') {
                            String oldVal = (String)sobj.get(field.APIName__c);
                            sobj.put(field.APIName__c, maskVariableLengthId(oldVal));
                        } 
                        // --- Standard Open-Source Actions ---
                        else {
                            MaskSObjectUtils.maskField(fieldType, field, sobj, data);
                        }
                    }
                }
            }

            SObjectType sObjType = ((SObject) Type.forName(this.sobjectName).newInstance()).getSObjectType();
            if (sObjType.getDescribe().isUpdateable()) {
                List<Database.SaveResult> srList = Database.update(scope, false);
                MaskSObjectUtils.saveErrors(srList, job.Id, this.sobjectName);
            }

            upsert new MaskSObject__c(APIName__c = this.sobjectName, TotalJobItems__c = job.TotalJobItems,
                JobItemsProcessed__c = job.JobItemsProcessed + 1, NumberOfErrors__c = job.NumberOfErrors) APIName__c;
        } catch (Exception e) {
            MaskSObjectUtils.saveError(e, job.Id, this.sobjectName);
        }
    }

    public void finish(Database.BatchableContext BC){
        upsert new MaskSObject__c(APIName__c = this.sobjectName, LastEnd__c = System.now()) APIName__c;
        if(sequence < lastSequence){
            Database.executeBatch(new MaskSObjectBatch(sequence), this.batchSize);
        }
    }

    private void setParams(Integer sequence, String sobjectName){
        if(sequence == null){
            sequence = 0;
        }

        List<MaskSObject__c> settings = MaskSObjectUtils.getMaskSObjectList(sequence, sobjectName);

        if(lastSequence == null){
            this.lastSequence = Integer.valueOf(settings.get(settings.size() - 1).Sequence__c);
        }

        MaskSObject__c setting = settings.get(0);
        this.sequence = Integer.valueOf(setting.Sequence__c);

        if(settings.size() > 1){
            this.batchSize = settings.get(1)?.BatchSize__c != null ? Integer.valueOf(settings.get(1).BatchSize__c) : MaskSObjectUtils.BATCH_SIZE;
        }

        this.sobjectName = setting.APIName__c;
        this.whereClause = setting.WhereClause__c;

        this.fields = setting.MaskSObjectFields__r;
        queryFields = new List<String>();

        setDictionaryIfRequied(setting);
        setFieldsTypes();
    }

    private void setFieldsTypes(){
        Schema.DescribeSObjectResult descr = Schema.getGlobalDescribe().get(this.sobjectName).getDescribe();
        Map<String, Schema.SObjectField> fields = descr.fields.getMap();

        fieldType = new Map<String, Schema.DisplayType>();
        for(String field : this.queryFields){
            Schema.DescribeFieldResult fr = fields.get(field.toLowerCase()).getDescribe();
            fieldType.put(field, fr.getType());
        }
    }

    private AsyncApexJob getJob(Id jobId){
        return [SELECT JobItemsProcessed, TotalJobItems, NumberOfErrors FROM AsyncApexJob WHERE Id=:jobId];
    }

    private void setDictionaryIfRequied(MaskSObject__c setting){
        Boolean requireDictionary = false;
        for(MaskSObjectField__c field : setting.MaskSObjectFields__r){
            this.queryFields.add(field.APIName__c);
            if(!requireDictionary && field.ActionType__c != null && field.ActionType__c.startsWith(MaskSObjectConstants.ACTION_TYPE_DICT)){
                requireDictionary = true;
            }
        }

        // --- CUSTOM: Parse into Combinatorics structure ---
        if(this.dictionary == null && requireDictionary){
            StaticResource sr = [SELECT Body FROM StaticResource WHERE Name = 'MaskSObjectDictionary' LIMIT 1];
            this.dictionary = (CombinatoricsDict)JSON.deserializeStrict(sr.Body.toString(), CombinatoricsDict.class);
        }
    }

    // --- CUSTOM HELPER METHODS ---

    private String generateFakePhone() {
        Integer areaCode = Math.mod(Math.abs(Crypto.getRandomInteger()), 800) + 200; 
        Integer prefix = Math.mod(Math.abs(Crypto.getRandomInteger()), 800) + 200;   
        Integer lineNum = Math.mod(Math.abs(Crypto.getRandomInteger()), 10000); 
        String lineStr = String.valueOf(lineNum).leftPad(4, '0');
        
        return '(' + areaCode + ') ' + prefix + '-' + lineStr;
    }

    private String maskVariableLengthId(String originalValue) {
        if (String.isBlank(originalValue)) {
            return originalValue; 
        }
        
        String chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
        String maskedString = '';
        
        for (Integer i = 0; i < originalValue.length(); i++) {
            Integer idx = Math.mod(Math.abs(Crypto.getRandomInteger()), chars.length());
            maskedString += chars.substring(idx, idx + 1);
        }
        
        return maskedString;
    }
}
