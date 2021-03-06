extern crate rusoto_core;
extern crate rusoto_dynamodb;

use std::collections::HashMap;
use std::str;

use rusoto_core::Region;
use rusoto_dynamodb::{
    AttributeValue, DescribeTableInput, DynamoDb, DynamoDbClient, GetItemInput, ListTablesInput,
};

#[tokio::main]
async fn main() {
    println!("Starting DynamoDb Examples");
    let client = DynamoDbClient::new(Region::UsEast1);

    // List the current DynamoDb Tables
    println!("\nExample: {} Starting...\n", "List DynamoDb Tables");
    list_dynamo_tables(&client).await;

    // Describe one of the tables
    println!("\nExample: {} Starting...\n", "Describe DynamoDb Table");
    describe_dynamodb_table(&client, "SensorData").await;

    // Get an item from the table
    println!("\nExample: {} Starting...\n", "Get DynamoDb Item");

    //
    // For some reason the example shown in the Rusoto test for DynamoDb uses the Key object but that doesn't work for me.  But if I understand the
    // source code correctly it's just a HashMap<String, AttributeValue>.  So here just create one of those and add the PK and SK values for the getItem
    // call.
    //
    let mut key: HashMap<String, AttributeValue> = HashMap::new();
    key.insert(
        String::from("SensorId"),
        AttributeValue {
            s: Some(String::from("28-000006b4e9ca")), // In this case SensorId is an "S"
            ..Default::default() // The rest of the fields in the AttributeValue struct are set to default
        },
    );
    key.insert(
        String::from("EpochTime"),
        AttributeValue {
            n: Some(String::from("1606148715")), // EpochTime is an "N"
            ..Default::default()
        },
    );
    let item_return = get_dynamodb_item(&client, "SensorData", key).await;
    println!("Item: {:?}", item_return);
}
// Function to get an item from a DynamoDb Table
//
// Output: GetItemOutput {
//                          consumed_capacity: None,
//                          item: Some({
//                                      "EpochTime": AttributeValue { b: None, bool: None, bs: None, l: None, m: None, n: Some("1606148715"), ns: None, null: None, s: None, ss: None },
//                                      "SensorId": AttributeValue { b: None, bool: None, bs: None, l: None, m: None, n: None, ns: None, null: None, s: Some("28-000006b4e9ca"), ss: None },
//                                      "Temperature": AttributeValue { b: None, bool: None, bs: None, l: None, m: None, n: Some("15.88"), ns: None, null: None, s: None, ss: None }}) }
//
async fn get_dynamodb_item(
    client: &DynamoDbClient,
    table_name: &str,
    keys: HashMap<String, AttributeValue>,
) -> HashMap<String, String> {

    let mut return_map: HashMap<String, String> = HashMap::new();

    let mut item_request = GetItemInput::default();
    item_request.key = keys;
    item_request.table_name = table_name.to_string();

    match client.get_item(item_request).await {
        Ok(output) => match output.item {
            Some(fields) => {
                for (k, v) in fields {
                    println!("{:?} {:?}", k, get_str_from_attribute(&v).unwrap());
                    return_map.insert(k, get_str_from_attribute(&v).unwrap().to_string());
                }
                return_map
            }
            None => {
                println!("No item found!");
                return_map
            }
        },
        Err(error) => {
            println!("Error: {:?}", error);
            return_map
        }
    }
}

fn get_str_from_attribute(attr: &AttributeValue) -> Option<&str> {
    match attr.b {
        None => (),
        Some(ref blob_attribute) => return Some(str::from_utf8(blob_attribute).unwrap()),
    }

    match attr.s {
        None => (),
        Some(ref string_attribute) => return Some(string_attribute),
    }

    match attr.n {
        None => (),
        Some(ref number_attribute) => return Some(number_attribute),
    }

    return None;
}

// Function to describe a DynamoDb Table
//
// Output: DescribeTableOutput {
//                                  table: Some(TableDescription {
//                                                                  archival_summary: None,
//                                                                  attribute_definitions: Some([AttributeDefinition { attribute_name: "EpochTime", attribute_type: "N" }, AttributeDefinition { attribute_name: "SensorId", attribute_type: "S" }]),
//                                                                  billing_mode_summary: Some(BillingModeSummary { billing_mode: Some("PAY_PER_REQUEST"), last_update_to_pay_per_request_date_time: Some(1606152097.504) }),
//                                                                  creation_date_time: Some(1606148416.268),
//                                                                  global_secondary_indexes: None,
//                                                                  global_table_version: None,
//                                                                  item_count: Some(43140),
//                                                                  key_schema: Some([KeySchemaElement { attribute_name: "SensorId", key_type: "HASH" }, KeySchemaElement { attribute_name: "EpochTime", key_type: "RANGE" }]),
//                                                                  latest_stream_arn: None,
//                                                                  latest_stream_label: None,
//                                                                  local_secondary_indexes: None,
//                                                                  provisioned_throughput: Some(ProvisionedThroughputDescription { last_decrease_date_time: None, last_increase_date_time: None, number_of_decreases_today: Some(0), read_capacity_units: Some(0), write_capacity_units: Some(0) }),
//                                                                  replicas: None,
//                                                                  restore_summary: None,
//                                                                  sse_description: None,
//                                                                  stream_specification: None,
//                                                                  table_arn: Some("arn:aws:dynamodb:us-east-1:XXXXXXXXXX:table/SensorData"),
//                                                                  table_id: Some("xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"),
//                                                                  table_name: Some("SensorData"),
//                                                                  table_size_bytes: Some(2240392),
//                                                                  table_status: Some("ACTIVE") }) }
//
// There is a lot of data returned from this, as shown above, but here I'm only printing the attribute_definitions
//
async fn describe_dynamodb_table(client: &DynamoDbClient, table_name: &str) {
    let mut input = DescribeTableInput::default();
    input.table_name = String::from(table_name);

    match client.describe_table(input).await {
        Ok(output) => match output.table {
            Some(table_descriptions) => match table_descriptions.attribute_definitions {
                Some(attribute_definitions) => {
                    for attribute_definition in attribute_definitions {
                        println!(
                            "Attribute Name: {}, Attribute Type: {}",
                            attribute_definition.attribute_name,
                            attribute_definition.attribute_type
                        )
                    }
                }
                None => println!("No attribute definitions found!"),
            },
            None => println!("No table description found!"),
        },
        Err(error) => {
            println!("Error: {:?}", error);
        }
    }
}

// Function to list the current DynamoDb tables in the AWS Account
//
// ListTablesOutput {   last_evaluated_table_name: None,
//                      table_names: Some(["SensorData", "SolarData", "credit_card"]) }
//
async fn list_dynamo_tables(client: &DynamoDbClient) {
    let list_tables_input: ListTablesInput = Default::default();

    match client.list_tables(list_tables_input).await {
        Ok(output) => match output.table_names {
            Some(table_name_list) => {
                println!("Tables in database:");
                for table_name in table_name_list {
                    println!("{}", table_name);
                }
            }
            None => println!("No tables in database!"),
        },
        Err(error) => {
            println!("Error: {:?}", error);
        }
    }
}
