package com.developery.azure;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Service;

import com.google.common.collect.Lists;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.table.CloudTable;
import com.microsoft.azure.storage.table.CloudTableClient;
import com.microsoft.azure.storage.table.TableOperation;
import com.microsoft.azure.storage.table.TableQuery;
import com.microsoft.azure.storage.table.TableQuery.Operators;
import com.microsoft.azure.storage.table.TableQuery.QueryComparisons;
import com.microsoft.azure.storage.table.TableResult;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class AzureStorageTableService {
	
	String storageConnectionString = "DefaultEndpointsProtocol=https;AccountName=developerytable;AccountKey=ECtIoLYSBDFgAsV4b4CiUPbpjC0tLKjljCASHuI3ljeA5xj83k4aw0tdGc0onOilKPpkZTLl3qkRfHX0G52jYg==;EndpointSuffix=core.windows.net";
	
	
	public boolean createTable(String tableName)
	{
		try
		{
		    // Retrieve storage account from connection-string.
		    CloudStorageAccount storageAccount =
		        CloudStorageAccount.parse(storageConnectionString);

		    // Create the table client.
		    CloudTableClient tableClient = storageAccount.createCloudTableClient();

		    CloudTable cloudTable = tableClient.getTableReference(tableName);
		    return cloudTable.createIfNotExists();
		}
		catch (Exception e)
		{
		    // Output the stack trace.
		    e.printStackTrace();
		}
		return false;
	}

	public List<String> getTableList() {

	    List<String> resList = new ArrayList<>();
		try
		{
		    // Retrieve storage account from connection-string.
		    CloudStorageAccount storageAccount =
		        CloudStorageAccount.parse(storageConnectionString);

		    // Create the table client.
		    CloudTableClient tableClient = storageAccount.createCloudTableClient();

		    
		    // Loop through the collection of table names.
		    for (String table : tableClient.listTables())
		    {
		        // Output each table name.
		        System.out.println(table);
		        resList.add(table);
		    }
		    
		}
		catch (Exception e)
		{
		    // Output the stack trace.
		    e.printStackTrace();
		}
		
		return resList;
	}
	
	public PeopleVO getPeople(String partitionKey, String rowKey) {

		try
		{
		    // Retrieve storage account from connection-string.
		    CloudStorageAccount storageAccount =
		        CloudStorageAccount.parse(storageConnectionString);

		    // Create the table client.
		    CloudTableClient tableClient = storageAccount.createCloudTableClient();

		    // Create a cloud table object for the table.
		    CloudTable cloudTable = tableClient.getTableReference("people");
		    
		    TableOperation retrieveOperation = TableOperation.retrieve(partitionKey, rowKey, PeopleVO.class);
		    
		    
		    // Submit the operation to the table service.
		    TableResult res = cloudTable.execute(retrieveOperation);
		    
		    PeopleVO people = res.getResultAsType();
		    
		    log.info("people: {}", people);
		    
		    
		    
		    return people;
		    
		}
		catch (Exception e)
		{
		    // Output the stack trace.
		    e.printStackTrace();
		}
		return null;
	
	}
	
	public ArrayList<PeopleVO> searchPeople(String name, int age) {

		try
		{
		    // Retrieve storage account from connection-string.
		    CloudStorageAccount storageAccount =
		        CloudStorageAccount.parse(storageConnectionString);

		    // Create the table client.
		    CloudTableClient tableClient = storageAccount.createCloudTableClient();

		    // Create a cloud table object for the table.
		    CloudTable cloudTable = tableClient.getTableReference("people");
		    
		    String nameFilter = TableQuery.generateFilterCondition(
		    		"Name",
		            QueryComparisons.EQUAL,
		            name);
		    
		    String ageFilter = TableQuery.generateFilterCondition(
		            "Age",
		            QueryComparisons.LESS_THAN,
		            age);
		    
		    String combinedFilter = TableQuery.combineFilters(nameFilter,
		            Operators.AND, ageFilter);
		    
		    log.info("combinedFilter: " + combinedFilter);
		    
		    TableQuery<PeopleVO> rangeQuery =
		            TableQuery.from(PeopleVO.class)
		            .where(combinedFilter);
		    
		    Iterable<PeopleVO> iter = cloudTable.execute(rangeQuery);
		    
		    ArrayList<PeopleVO> list = Lists.newArrayList(iter);
	        // Loop through the results, displaying information about the entity
	        for (PeopleVO entity : list) {
	            log.info("name: {}, age: {}", entity.getName(), entity.getAge(), entity.getAddr());
	        }
		    
		    
		    
		    return list;
		    
		}
		catch (Exception e)
		{
		    // Output the stack trace.
		    e.printStackTrace();
		}
		return null;
	
	}
	
	
	public String insertPeople(PeopleVO p)
	{
		System.out.println(p);
		try
		{
		    // Retrieve storage account from connection-string.
		    CloudStorageAccount storageAccount =
		        CloudStorageAccount.parse(storageConnectionString);

		    // Create the table client.
		    CloudTableClient tableClient = storageAccount.createCloudTableClient();

		    // Create a cloud table object for the table.
		    CloudTable cloudTable = tableClient.getTableReference("people");
		    // unique 한 값조합이 되도록 아래 2개 세팅
		    p.setPartitionKey(p.getId());
		    p.setRowKey(p.getId());

		    TableOperation insertCustomer1 = TableOperation.insertOrReplace(p);		    

		    // Submit the operation to the table service.
		    TableResult res = cloudTable.execute(insertCustomer1);		    
		    
		    System.out.println(res.getHttpStatusCode());
		    
		}
		catch (Exception e)
		{
		    // Output the stack trace.
		    e.printStackTrace();
		}
		return p.getId() + "/" + p.getId();
	}
}
