const fs = require('fs')
const csv = require('csv-parser')
//const fastcsv = require('fast-csv')


const arrayOfObjectsToObjectKayValue = (arrayOfObjects, key, value) => {
  const objectKeyValue = {}
  arrayOfObjects.forEach(element => {
      objectKeyValue[element[key]]=element[value]
  })
  return objectKeyValue
}

const readDataFromCsvPromise = (filePath) => {
    return new Promise(function(resolve, reject) {
    const tab = []
    fs.createReadStream(filePath)
    .pipe(csv())
    .on('data', (row) => {
      tab.push(row) 
    })
    .on('end', () => {
        resolve(tab)
    })
    })
}

const readIndexedDataFromCsvPromise = (filePath, key) => {
    return new Promise(function(resolve, reject) {
      const tab = {}
      fs.createReadStream(filePath)
      .pipe(csv())
      .on('data', (row) => {
        if(!tab[row[key]]){
            tab[row[key]] = []
            tab[row[key]].push(row)
        }else{
            tab[row[key]].push(row)
        }
        //tab[row[key]]=row 
      })
      .on('end', () => {
          resolve(tab)
      })
    })
  }

const readAllCsvsPromise = async (filesPaths) => {
    const promises=[]
    filesPaths.forEach(filePath => {
        promises.push(readDataFromCsvPromise(filePath))
    });
    const data=await Promise.all(promises)
    const resData = []
    for (let i=0; i<data.length;i++){
        resData[filesPaths[i].split('/')[2].split(".")[0]]=data[i]  
    }
    return resData
}

const readAllCsvsIndexedPromise = async (files) => {
    const promises=[]
    files.forEach(file=> {
        promises.push(readIndexedDataFromCsvPromise(file["filePath"], file["key"]))
    });
    const data=await Promise.all(promises)
    const resData = []
    for (let i=0; i<data.length;i++){

        resData[files[i]["filePath"].split('/')[2].split(".")[0]]=data[i]  
    }
    return resData
}

const getFilesPaths = (folderPath) => {
    const paths=[]
    fs.readdirSync(folderPath).forEach(fileName => {
        paths.push(`${folderPath}/${fileName}`);
    })
    return paths
}

const handle_CapitalLowerDrawnsix_Form = (message) => {

    let arrayWords = message.split(" ")

    let res = ""

    arrayWords.forEach(word=>{
    res = res+word.charAt(0).toUpperCase() + word.slice(1).toLowerCase()+"-"
    })

    return res.slice(0, -1)
}


module.exports= {readAllCsvsPromise, readAllCsvsIndexedPromise, arrayOfObjectsToObjectKayValue, getFilesPaths, handle_CapitalLowerDrawnsix_Form}

