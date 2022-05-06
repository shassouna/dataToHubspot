//Authenticate via API Key 
const hubspot = require('@hubspot/api-client')
const hubspotClient = new hubspot.Client({ apiKey: `c721e8e5-be78-4b44-8dd1-8d3c3abe4bb8` })

//Example call
hubspotClient.crm.contacts.basicApi
    .getPage(limit, after, properties, associations, archived)
    .then((results) => {
        console.log(results.body)
    })
    .catch((err) => {
        console.error(err)
    })