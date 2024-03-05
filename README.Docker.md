## Code Test 
### Building and running your application

When you're ready, start your application by running:
`docker compose up --build`.

Your application will be available at http://localhost:8000/docs.

### Testing

1. Access the application available on http://localhost:8000/docs, click in Authorize. Use the authorization value = 'LEONARDO123'
2. After authentication, access the endpoint csv_data_ingestion to load and transform all the data from the CSV file.
3. Use the endpoints get_data or get_all_data to extract part or all of the data, if needed.
4. Two additional endpoints have been created to extract information from the imported dataset:
    1. get_category_by_age: Extract information on how many entries there are for each age group in each category type (address_category_water, address_category_relief, address_category_flat).
    2. get_age_amount: Extract information on how many entries there are for each age bracket in the dataset.




