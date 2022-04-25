const fs = require('fs')
const fastcsv = require('fast-csv')
const { convertArrayToCSV } = require('convert-array-to-csv');

const global_functions = require('./models/global_functions');
const { Console } = require('console');

// get array of all paths corresponding to each file  
const files_paths = global_functions.getFilesPaths("./bdd_tables_mini")


const handleBuildArrayObjectsHubspot = (tables) => {
    
    // final array of object to hubspot
    const finalArrayOfObjects = []

    //const objHubspot = {}

    tables["EXPOSANT"].forEach(exposant => {

        // data from EXPOSANT
        /*objHubspot["nom"] = exposant["NOM_NETTOYE"],
        objHubspot["autorisation"] = exposant["AUTORISATION"],
        objHubspot["raison_sociale"] = exposant["RAISON_SOCIALE"],
        objHubspot["rib_compte"] = exposant["RIB_COMPTE"],
        objHubspot["cle_exposant"] = exposant["CLE_EXPOSANT"],
        objHubspot["site_web"] = exposant["SRV_INTERNET"],
        objHubspot["date_autorisation"] = exposant["DATE_AUTORI"],
        objHubspot["tva_intercommunautaire"] = exposant["TVA_INTRACOMMUNAUTAIRE"],
        objHubspot["leader"] = exposant["LEADER"],
        objHubspot["qualite"]= exposant["QUALITE"],
        objHubspot["site_marchand"]= exposant["SITE_MARCHAND"],
        objHubspot["invite"]= exposant["INVITE"]*/
        const objHubspot = {
            nom : exposant["NOM_NETTOYE"],
            autorisation : exposant["AUTORISATION"],
            raison_social : exposant["RAISON_SOCIALE"],
            rib_compte : exposant["RIB_COMPTE"],
            cle_exposant : exposant["CLE_EXPOSANT"],
            site_web : exposant["SRV_INTERNET"],
            date_autorisation : exposant["DATE_AUTORI"],
            tva_intercommunautaire : exposant["TVA_INTRACOMMUNAUTAIRE"],
            leader : exposant["LEADER"],
            qualite : exposant["QUALITE"],
            site_marchand : exposant["SITE_MARCHAND"],
            invite : exposant["INVITE"]
        }

        // data from PAYS
        try{
            const paysExposant = tables["PAYS"].find(pays=>pays["CLE_PAYS"]==exposant["PAYS"])
            objHubspot["pays"] = paysExposant["LIB_FR"]
        }
        catch(error){
            console.log("error in data from PAYS")
        }

        // data from SALON_EXPO
        try{
            const salons=[]
            const tab=tables["SALON_EXPO"].filter((salon)=>salon["CLE_EXPOSANT"] == exposant["CLE_EXPOSANT"])
            tab.forEach(salonFiltered=>{
                salons.push({lieu : salonFiltered["LIEU"], date : salonFiltered["DATE"]})
                //objHubspot[`salon/${tab.indexOf(salonFiltered)}`] = `lieu : ${salonFiltered["LIEU"]} / date : ${salonFiltered["DATE"]}`         
            })
             objHubspot["salons"] = convertArrayToCSV(salons)
        }
        catch(error){
            //console.log("error in data from SALON_EXPO")
        }

        // data from AMBIANCE
        try{
            const ambianceExposant = tables["AMBIANCE"].find(ambiance=>ambiance["CLE_AMBIANCE"] == exposant["CLE_AMBIANCE"])
            objHubspot["ambiance"] = ambianceExposant["LIB_FR"]       
        } 
        catch(error) {
            //console.log("error in data from AMBIANCE")            
        }

        // data from RAYON_DETAILS rayon principale
        try{
            const rayonPrincipaleExposant = tables["RAYON_DETAILS"].find(rayon=>rayon["CLE_RAYON"] == exposant["CLE_RAYON_PRINCIPAL"])
            objHubspot["rayon_principale"] = rayonPrincipaleExposant["LIB"]
        }
        catch(error){
            //console.log("error in data from RAYON_DETAILS rayon principale")
        }

        // data from RAYON_DETAILS rayons
        try{
            const linksRayonsExposant = tables["LIEN_EXP_RAYON"].filter(rayon=>rayon["CLE_EXPOSANT"] == exposant["CLE_EXPOSANT"])
            
            const rayons=[]
            linksRayonsExposant.forEach(linkRayon => {
                const rayon = tables["RAYON_DETAILS"].find(rayon=>rayon["CLE_RAYON"] == linkRayon["CLE_RAYON"])
                rayons.push(rayon["LIB"])
            })
            objHubspot["rayons_expo"] = convertArrayToCSV([rayons])
        }
        catch(error){
            //console.log("error in data from RAYON_DETAILS rayons")
        }

        // data from LIEN_EXPOSANT_EXPOSANT
        try{
            const links=[]
            tables["LIEN_EXPOSANT_EXPOSANT"].filter(link=>link["cle_exposant_parent"]==exposant["CLE_EXPOSANT"])
            .forEach((linkRes)=>{        
                const nomLinkedExposant = tables["EXPOSANT"].find(exp=>exp["CLE_EXPOSANT"] == linkRes["cle_exposant_enfant"])
                links.push({exposant:nomLinkedExposant["NOM"], relation:linkRes["relation"]})
            })
            tables["LIEN_EXPOSANT_EXPOSANT"].filter(link=>link["cle_exposant_enfant"]==exposant["CLE_EXPOSANT"])
            .forEach((linkRes2)=>{        
                const nomLinkedExposant2 = tables["EXPOSANT"].find(exp=>exp["CLE_EXPOSANT"] == linkRes2["cle_exposant_parent"])
                if(!links.find(element=>element["exposant"]==nomLinkedExposant["NOM"] && element["relation"]==linkRes2["relation"])){
                    links.push({exposant:nomLinkedExposant2["NOM"], relation:linkRes2["relation"]})
                }
            })
            objHubspot["relation_expo"] = convertArrayToCSV(links)
        }
        catch(error){
            //console.log("error in data from LIEN_EXPOSANT_EXPOSANT")
        }


        // data from STATUT_COMMERCIAL
        try {
           objHubspot["statut_commercial"] = tables["STATUT_COMMERCIAL"].find(statut=>statut["ID"] == exposant["STATUT_COMMERCIAL"])["lib"]

        } 
        catch(error) {
            //console.log("error in data from STATUT_COMMERCIAL")
        }

        // data from TYPE_EXP
        try {
            const cleTypeExposant = tables["LIEN_EXP_TYP"].find(link=>link["CLE_EXPOSANT"] == exposant["CLE_EXPOSANT"])["CLE_TYPE_EXP"]
            objHubspot["type_exposant"] = tables["TYPE_EXP"].find(typeExp=>typeExp["CLE_TYPE_EXP"] == cleTypeExposant)["LIB_FR"]
        }
        catch(error){
            //console.log("error in data from TYPE_EXP")
        }

        // data from EXPOSANT_CONTACT_STATUT
        try {
            const findCleContactExposant = tables["EXPOSANT_CONTACT"].find(contact=>contact["cle_exposant"]==exposant["CLE_EXPOSANT"])["cle_exposant_contact"]

            const cleStatut = tables["LIEN_EXPOSANT_CONTACT_STATUT"].find(link=>link["cle_exposant_contact"]== findCleContactExposant)["cle_statut"]
            objHubspot["statut"] = tables["EXPOSANT_CONTACT_STATUT"].find(statut=>statut["cle_statut"]==cleStatut)["abreviation"]
            //console.log(objHubspot["statut"])
        }
        catch(error){
            //console.log("error in data from EXPOSANT_CONTACT_STATUT")
        }

        // data from EXPOSANT_CONTACT
       /* try {
            //const contactExposant = tables["EXPOSANT_CONTACT"].find(contact=>contact["cle_exposant"]==exposant["CLE_EXPOSANT"])
            const contactsExposant = tables["EXPOSANT_CONTACT"].filter(contact=>contact["cle_exposant"]==exposant["CLE_EXPOSANT"])
            objHubspot["nom_contact"] = contactsExposant[0]["nom"]
            objHubspot["prenom_contact"] = contactsExposant[0]["prenom"]
            objHubspot["telephone"] = contactsExposant[0]["telephone"]
            objHubspot["portable"] = contactsExposant[0]["portable"]
            objHubspot["email"] = contactsExposant[0]["email"]

            for(let i=1 ; i<contactsExposant.length ; i++) {
                objHubspot["nom_contact"+i] = contactsExposant[i]["nom"]
                objHubspot["prenom_contact"+i] = contactsExposant[i]["prenom"]
                objHubspot["telephone"+i] = contactsExposant[i]["telephone"]
                objHubspot["portable"+i] = contactsExposant[i]["portable"]
                objHubspot["email"+i] = contactsExposant[i]["email"]              
            }
        } 
        catch(error) {
            //console.log("error in data from EXPOSANT_CONTACT")
        }*/
        finalArrayOfObjects.push(objHubspot)
    })   
    return finalArrayOfObjects
}
const asyncFunc = async (files_paths) => {

    // get array of each file data in json format
    const tables = await global_functions.readAllCsvsPromise(files_paths)

    // build an array of objects (each object refer to one 'exposant')
    const data = handleBuildArrayObjectsHubspot(tables)
    const ws = fs.createWriteStream("result2.csv");
    fastcsv
      .write(data, { headers: true })
      .pipe(ws);

    /*let dataJSON = JSON.stringify(data)
    fs.writeFileSync('result2.json', dataJSON)*/
}
const files = [
    {filePath:"./bdd_tables_mini/AMBIANCE.csv", key:"CLE_AMBIANCE"},
    {filePath:"./bdd_tables_mini/EXPOSANT_CONTACT_STATUT.csv", key:"cle_statut"},
    {filePath:"./bdd_tables_mini/EXPOSANT_CONTACT.csv", key:"cle_exposant"},
    {filePath:"./bdd_tables_mini/EXPOSANT.csv", key:"CLE_EXPOSANT"},
    {filePath:"./bdd_tables_mini/LIEN_EXP_RAYON.csv", key:"CLE_EXPOSANT"},
    {filePath:"./bdd_tables_mini/LIEN_EXP_TYP.csv", key:"CLE_EXPOSANT"},
    {filePath:"./bdd_tables_mini/LIEN_EXPOSANT_CONTACT_STATUT.csv", key:"cle_exposant_contact"},
    {filePath:"./bdd_tables_mini/LIEN_EXPOSANT_EXPOSANT_PARENT.csv", key:"cle_exposant_parent"},
    {filePath:"./bdd_tables_mini/LIEN_EXPOSANT_EXPOSANT_FILS.csv", key:"cle_exposant_enfant"},
    {filePath:"./bdd_tables_mini/PAYS.csv", key:"CLE_PAYS"},
    {filePath:"./bdd_tables_mini/RAYON_DETAILS.csv", key:"CLE_RAYON"},
    {filePath:"./bdd_tables_mini/SALON_EXPO.csv", key:"CLE_EXPOSANT"},
    {filePath:"./bdd_tables_mini/STATUT_COMMERCIAL.csv", key:"ID"},
    {filePath:"./bdd_tables_mini/TYPE_EXP.csv", key:"CLE_TYPE_EXP"}
]
const asyncFunc2 = async (files) => {
    // consts
    const EXPOSANT_TABLE = "EXPOSANT"
    const PAYS_TABLE = "PAYS"
    const SALON_EXPO_TABLE = "SALON_EXPO"
    const AMBIANCE = "AMBIANCE"
    const RAYON_DETAILS_TABLE = "RAYON_DETAILS"
    const EXPOSANT_CONTACT_STATUT_TABLE = "EXPOSANT_CONTACT_STATUT"
    const LIEN_EXPOSANT_CONTACT_STATUT_TABLE = "LIEN_EXPOSANT_CONTACT_STATUT"
    const EXPOSANT_CONTACT_TABLE = "EXPOSANT_CONTACT"
    const LIEN_EXP_RAYON_TABLE = "LIEN_EXP_RAYON"
    const LIEN_EXPOSANT_EXPOSANT_PARENT_TABLE = "LIEN_EXPOSANT_EXPOSANT_PARENT"
    const LIEN_EXPOSANT_EXPOSANT_FILS_TABLE = "LIEN_EXPOSANT_EXPOSANT_FILS"
    const LIEN_EXP_TYP_TABLE = "LIEN_EXP_TYP"
    const STATUT_COMMERCIAL_TABLE = "STATUT_COMMERCIAL"
    const TYPE_EXP_TABLE = "TYPE_EXP"

    const tables = await global_functions.readAllCsvsIndexedPromise(files)

    for (const key in tables[EXPOSANT_TABLE]){

        // data from EXPOSANT
        const objHubspot = {
            nom : tables[EXPOSANT_TABLE][key][0]["NOM_NETTOYE"],
            autorisation : tables[EXPOSANT_TABLE][key][0]["AUTORISATION"],
            raison_social : tables[EXPOSANT_TABLE][key][0]["RAISON_SOCIALE"],
            rib_compte : tables[EXPOSANT_TABLE][key][0]["RIB_COMPTE"],
            cle_exposant : tables[EXPOSANT_TABLE][key][0]["CLE_EXPOSANT"],
            site_web : tables[EXPOSANT_TABLE][key][0]["SRV_INTERNET"],
            date_autorisation : tables[EXPOSANT_TABLE][key][0]["DATE_AUTORI"],
            tva_intercommunautaire : tables[EXPOSANT_TABLE][key][0]["TVA_INTRACOMMUNAUTAIRE"],
            leader : tables[EXPOSANT_TABLE][key][0]["LEADER"],
            qualite : tables[EXPOSANT_TABLE][key][0]["QUALITE"],
            site_marchand : tables[EXPOSANT_TABLE][key][0]["SITE_MARCHAND"],
            invite : tables[EXPOSANT_TABLE][key][0]["INVITE"]
        }
   // console.log(objHubspot)
        // data from PAYS
        try{
            objHubspot["pays"] = tables[PAYS_TABLE][tables[EXPOSANT_TABLE][key][0]["PAYS"]][0]["LIB_FR"]
        }
        catch(error){
            //console.log("error in data from PAYS")
        }  
  
        // data from SALON_EXPO        
        try{
            objHubspot["salons_expo"] = Array.from(new Set(tables[SALON_EXPO_TABLE][key]))
                                        .map(val=> {
                                            return {lieu : val["LIEU"], date : val["DATE"]}
                                        })
        }
        catch(error){
            //console.log("error in data from SALON_EXPO")
        }  

        // data from AMBIANCE
        try{
            objHubspot["ambiance"] = tables[AMBIANCE][tables[EXPOSANT_TABLE][key][0]["CLE_AMBIANCE"]][0]["LIB_FR"]
        }
        catch(error){
            //console.log("error in data from AMBIANCE")
        }  

        // data from RAYON_DETAILS rayon principale
        try{
            objHubspot["rayon_principale"] = tables[RAYON_DETAILS_TABLE][tables[EXPOSANT_TABLE][key][0]["CLE_RAYON_PRINCIPAL"]][0]["LIB"]
        }
        catch(error){
            //console.log("error in data from RAYON_DETAILS rayon principale")
        }

        // data from RAYON_DETAILS rayons
        try{
            const rayons = []
            tables[LIEN_EXP_RAYON_TABLE][key].forEach(rayon => {      
                rayons.push(tables[RAYON_DETAILS_TABLE][rayon["CLE_RAYON"]][0]["LIB"])       
            })
            objHubspot["rayons_expo"] = rayons
        }
        catch(error){
            //console.log("error in data from RAYON_DETAILS rayons")
        }

        // data from LIEN_EXPOSANT_EXPOSANT
        try{
            const links=[]
            tables[LIEN_EXPOSANT_EXPOSANT_PARENT_TABLE][key].forEach(element => {
                links.push({
                    nom : tables[EXPOSANT_TABLE][element["cle_exposant_enfant"]][0]["NOM"],
                    relation : element["relation"]
                })
            })
            tables[LIEN_EXPOSANT_EXPOSANT_FILS_TABLE][key].forEach(element => {
                links.push({
                    nom : tables[EXPOSANT_TABLE][element["cle_exposant_parent"]][0]["NOM"],
                    relation : element["relation"]
                })
            })
            objHubspot["relation_expo"] = links
        }
        catch(error){
            //console.log("error in data from LIEN_EXPOSANT_EXPOSANT")
        }

        // data from STATUT_COMMERCIAL
        try {
            objHubspot["statut_commercial"] = tables[STATUT_COMMERCIAL_TABLE][tables[EXPOSANT_TABLE][key][0]["STATUT_COMMERCIAL"]][0]["lib"]
        } 
         catch(error) {
             //console.log("error in data from STATUT_COMMERCIAL")
        }

        // data from TYPE_EXP
        try {
            const cleTypeExposant = tables[LIEN_EXP_TYP_TABLE][key][0]["CLE_TYPE_EXP"]
            objHubspot["type_exposant"] = tables[TYPE_EXP_TABLE][cleTypeExposant][0]["LIB_FR"]
        }
        catch(error){
            //console.log("error in data from TYPE_EXP")
        }
        //console.log(objHubspot)

        // data from EXPOSANT_CONTACT_STATUT
        try {

            const cleContactExposant = tables[EXPOSANT_CONTACT_TABLE][key][0]["cle_exposant_contact"]

            const cleStatut = tables[LIEN_EXPOSANT_CONTACT_STATUT_TABLE][cleContactExposant][0]["cle_statut"]
            objHubspot["statut"] = tables[EXPOSANT_CONTACT_STATUT_TABLE][cleStatut][0]["abreviation"]

        }
        catch(error){
            //console.log("error in data from EXPOSANT_CONTACT_STATUT")
        }
        console.log(objHubspot)
    }
}

asyncFunc2(files)




