package com.developery.azure;

import com.microsoft.azure.storage.table.TableServiceEntity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PeopleVO extends TableServiceEntity {

	String id;
	String name;
	String addr;
	int age;
}
