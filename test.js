const { convertArrayToCSV } = require('convert-array-to-csv');
//const converter = require('convert-array-to-csv');
const fs = require('fs')
const csv = require('csv-parser')
 
/*const header = ['number', 'first', 'last', 'handle'];
const dataArrays = [
  [1, 'Mark', 'Otto', '@mdo'],
  [2, 'Jacob', 'Thornton', '@fat'],
  [3, 'Larry', 'the Bird', '@twitter'],
];
const dataObjects = [
  {
    number: 1,
    first: 'Mark',
    last: 'Otto',
    handle: '@mdo',
  },
  {
    number: 2,
    first: 'Jacob',
    last: 'Thornton',
    handle: '@fat',
  },
  {
    number: 3,
    first: 'Larry',
    last: 'the Bird',
    handle: '@twitter',
  },
];
 

  const csvFromArrayOfObjects  = 'number,first,last,handle\n1,Mark,Otto,@mdo\n2,Jacob,Thornton,@fat\n3,Larry,the Bird,@twitter\n';

const csvFromArrayOfObjects =convertArrayToCSV(dataObjects)
const csvFromArrayOfArrays = convertArrayToCSV(dataArrays, {
  //header,
  separator: ';'
});
console.log(csvFromArrayOfObjects)
console.log(csvFromArrayOfArrays)

const readDataFromCsvPromise = () => {
    const tab = []
    const tabEmail = []
    const tabEmailAutorise = []
    fs.createReadStream("./bdd_tables/EXPOSANT_CONTACT.CSV")
    .pipe(csv())
    .on('data', (row) => {
      tab.push(row) 
      if (row.email) tabEmail.push(row)
    })
    .on('end', () => {
        fs.createReadStream("./bdd_tables/EXPOSANT.CSV")
        .pipe(csv())
        .on('data', (row) => {
            if(row["AUTORISATION"]=="1"){
                if(tabEmail.find(element=>element["cle_exposant"]==row["CLE_EXPOSANT"]))
                tabEmailAutorise.push(tabEmail.find(element=>element["cle_exposant"]==row["CLE_EXPOSANT"]))
            }
        })
        .on('end', () => {
    
            console.log(tab)
            console.log(tabEmail)
            console.log(tabEmailAutorise)
            console.log(tab.length)
            console.log(tabEmail.length)
            console.log(tabEmailAutorise.length)
        })
    })
}
readDataFromCsvPromise()*/
const readIndexedDataFromCsvPromise = (filePath, key) => {
  return new Promise(function(resolve, reject) {
    const tab = {}
    fs.createReadStream(filePath)
    .pipe(csv())
    .on('data', (row) => {
      tab[row[key]]=row 
    })
    .on('end', () => {
        resolve(tab)
    })
  })
}
/*const csv = require('csv-parser')
const tab = []
fs.createReadStream("out.csv")
.pipe(csv())
.on('data', (row) => {
  console.log(row.rayons_expo)
  tab.push(row) 
})*/
readIndexedDataFromCsvPromise("./bdd_tables/PAYS.csv", "CLE_PAYS")
.then(d=>console.log(d))