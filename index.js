const fs = require('fs')
const fastcsv = require('fast-csv')
const { convertArrayToCSV } = require('convert-array-to-csv');

const global_functions = require('./models/global_functions');

const asyncFunc = async () => {

    // consts
    const folderName = "bdd_tables"
    const files = [
        {filePath:`./${folderName}/AMBIANCE.csv`, key:"CLE_AMBIANCE"},
        {filePath:`./${folderName}/EXPOSANT_CONTACT_STATUT.csv`, key:"cle_statut"},
        {filePath:`./${folderName}/EXPOSANT_CONTACT.csv`, key:"cle_exposant"},
        {filePath:`./${folderName}/EXPOSANT.csv`, key:"CLE_EXPOSANT"},
        {filePath:`./${folderName}/LIEN_EXP_RAYON.csv`, key:"CLE_EXPOSANT"},
        {filePath:`./${folderName}/LIEN_EXP_TYP.csv`, key:"CLE_EXPOSANT"},
        {filePath:`./${folderName}/LIEN_EXPOSANT_CONTACT_STATUT.csv`, key:"cle_exposant_contact"},
        {filePath:`./${folderName}/LIEN_EXPOSANT_EXPOSANT_PARENT.csv`, key:"cle_exposant_parent"},
        {filePath:`./${folderName}/LIEN_EXPOSANT_EXPOSANT_FILS.csv`, key:"cle_exposant_enfant"},
        {filePath:`./${folderName}/PAYS.csv`, key:"CLE_PAYS"},
        {filePath:`./${folderName}/RAYON_DETAILS.csv`, key:"CLE_RAYON"},
        {filePath:`./${folderName}/SALON_EXPO.csv`, key:"CLE_EXPOSANT"},
        {filePath:`./${folderName}/STATUT_COMMERCIAL.csv`, key:"ID"},
        {filePath:`./${folderName}/TYPE_EXP.csv`, key:"CLE_TYPE_EXP"},
        {filePath:`./${folderName}/EXPOSANT_NEWSLETTER.csv`, key:"CLE_EXPOSANT"},
        {filePath:`./${folderName}/NL.csv`, key:"CLE_NL"},
        {filePath:`./${folderName}/LIEN_REVENDEUR.csv`, key:"CLE_EXPOSANT_REVENDEUR"},
        {filePath:`./${folderName}/FACTURE.csv`, key:"CLE_EXPOSANT"},
    ]

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
    const EXPOSANT_NEWSLETTER = "EXPOSANT_NEWSLETTER"
    const NL = "NL"
    const LIEN_REVENDEUR = "LIEN_REVENDEUR"
    const FACTURE = "FACTURE"

    const result = []

    const tables = await global_functions.readAllCsvsIndexedPromise(files)

    for (const key in tables[EXPOSANT_TABLE]){

        // data from EXPOSANT
        const objHubspot = {
            nom : tables[EXPOSANT_TABLE][key][0]["NOM_NETTOYE"],
            nom_hubspot : global_functions.handle_CapitalLowerDrawnsix_Form(tables[EXPOSANT_TABLE][key][0]["NOM_NETTOYE"]),
            date_autorisation : tables[EXPOSANT_TABLE][key][0]["DATE_AUTORI"],
            site_web : tables[EXPOSANT_TABLE][key][0]["SRV_INTERNET"],
            univers_principal:null,
            univers_expo:null,
            salons_expo:null,
            pays:null,
            ville:tables[EXPOSANT_TABLE][key][0]["VILLE"],
            adresse:tables[EXPOSANT_TABLE][key][0]["ADRESSE"],
            code_postale:tables[EXPOSANT_TABLE][key][0]["CP"],
            fax:tables[EXPOSANT_TABLE][key][0]["FAX"],
            statut_commercial:null,
            type_exposant:null,
            ambiance:null,
            relation_expo:null,
            autorisation : tables[EXPOSANT_TABLE][key][0]["AUTORISATION"],
            raison_sociale : tables[EXPOSANT_TABLE][key][0]["RAISON_SOCIALE"],
            rib_compte : tables[EXPOSANT_TABLE][key][0]["RIB_COMPTE"],
            cle_exposant : tables[EXPOSANT_TABLE][key][0]["CLE_EXPOSANT"],
            tva_intercommunautaire : tables[EXPOSANT_TABLE][key][0]["TVA_INTRACOMMUNAUTAIRE"],
            leader : tables[EXPOSANT_TABLE][key][0]["LEADER"],
            qualite : tables[EXPOSANT_TABLE][key][0]["QUALITE"],
            site_marchand : tables[EXPOSANT_TABLE][key][0]["SITE_MARCHAND"],
            facebook : tables[EXPOSANT_TABLE][key][0]["FACEBOOK"],
            twitter : tables[EXPOSANT_TABLE][key][0]["TWITTER"],
            instagram : tables[EXPOSANT_TABLE][key][0]["INSTAGRAM"],
            pinterest : tables[EXPOSANT_TABLE][key][0]["PINTEREST"],
            youtube : tables[EXPOSANT_TABLE][key][0]["YOUTUBE"],
            vimeo : tables[EXPOSANT_TABLE][key][0]["VIMEO"],
            desc_fr : tables[EXPOSANT_TABLE][key][0]["DESC_FR"],
            desc_gb : tables[EXPOSANT_TABLE][key][0]["DESC_GB"],
            desc_de : tables[EXPOSANT_TABLE][key][0]["DESC_DE"],
            desc_it : tables[EXPOSANT_TABLE][key][0]["DESC_IT"],
            desc_es : tables[EXPOSANT_TABLE][key][0]["DESC_ES"],

            //invite : tables[EXPOSANT_TABLE][key][0]["INVITE"]
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
            objHubspot["salons_expo"] = convertArrayToCSV(Array.from(new Set(tables[SALON_EXPO_TABLE][key]))
                                        .map(val=> {
                                            return {lieu : val["LIEU"], date : val["DATE"]}
                                        }))
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
            objHubspot["univers_principal"] = tables[RAYON_DETAILS_TABLE][tables[EXPOSANT_TABLE][key][0]["CLE_RAYON_PRINCIPAL"]][0]["LIB"]
            //console.log(objHubspot["univers_principal"])
        }
        catch(error){
            //console.log("error in datar fom RAYON_DETAILS rayon principale")    
        }

        // data from RAYON_DETAILS rayons
        try{
            const rayons = []
            tables[LIEN_EXP_RAYON_TABLE][key].forEach(rayon => {      
                rayons.push(tables[RAYON_DETAILS_TABLE][rayon["CLE_RAYON"]][0]["LIB"])       
            })
            objHubspot["univers_expo"] = convertArrayToCSV([rayons])
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
            objHubspot["relation_expo"] = convertArrayToCSV([links])
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
        
        // data from NL 
        try {
            const newsletters = []
            tables[EXPOSANT_NEWSLETTER][key].forEach(expNl => {
                newsletters.push({
                    NOM_NL : tables[NL][expNl["CLE_NL"]][0]["NOM_NL"],
                    lib_fr : tables[NL][expNl["CLE_NL"]][0]["lib_fr"],
                    lib_gb : tables[NL][expNl["CLE_NL"]][0]["lib_gb"],
                    lib_de : tables[NL][expNl["CLE_NL"]][0]["lib_de"],
                    lib_es : tables[NL][expNl["CLE_NL"]][0]["lib_es"],
                    lib_it : tables[NL][expNl["CLE_NL"]][0]["lib_it"],
                    superunivers_fr : tables[NL][expNl["CLE_NL"]][0]["superunivers_fr"],
                    superunivers_gb : tables[NL][expNl["CLE_NL"]][0]["superunivers_gb"],
                    superunivers_de : tables[NL][expNl["CLE_NL"]][0]["superunivers_de"],
                    superunivers_es : tables[NL][expNl["CLE_NL"]][0]["superunivers_es"],
                    superunivers_it : tables[NL][expNl["CLE_NL"]][0]["superunivers_it"],
                })
            })
            objHubspot["newsletters"] = convertArrayToCSV([newsletters])
        }
        catch(error){
            console.log("error in data from NL")
        }

        // data from NL 
        try {
            const factures = []
            tables[FACTURE][key].forEach(facture => {
                factures.push({
                    ID_FACTURE: facture["ID_FACTURE"],
                    NOM: facture["NOM"],
                    CONTACT: facture["CONTACT"],
                    ADRESSE: facture["ADRESSE"],
                    CP: facture["CP"],
                    VILLE: facture["VILLE"],
                    PAYS: facture["PAYS"],
                    TEL: facture["TEL"],
                    FAX: facture["FAX"],
                    SRV_INTERNET: facture["SRV_INTERNET"],
                    EMAIL: facture["EMAIL"],
                    DATE_EMISSION: facture[""],
                    DIVERS: facture["DATE_EMISSION"],
                    PRIX_HT: facture["PRIX_HT"],
                    REMISE: facture["REMISE"],
                    NET: facture["NET"],
                    CLE_TVA: facture["CLE_TVA"],
                    CLE_DEVISE: facture["CLE_DEVISE"],
                    RATIO_DEVISE: facture["RATIO_DEVISE"],
                    TOTAL_TTC: facture["TOTAL_TTC"],
                    AUTORISATION: facture["AUTORISATION"],
                    TYPE_BDC: facture["TYPE_BDC"],
                    PP_MOIS: facture["PP_MOIS"],
                    DATE_DEBUT: facture["DATE_DEBUT"],
                    PAIEMENT_TTC: facture["PAIEMENT_TTC"],
                    DATE_PAIEMENT: facture["DATE_PAIEMENT"],
                    PRELEVEMENT_PAS_PAYE_TTC: facture["PRELEVEMENT_PAS_PAYE_TTC"],
                    ID_FACTURE_POUR_AVOIR: facture["ID_FACTURE_POUR_AVOIR"],
                    SKIN: facture["SKIN"],
                    LETTRAGE: facture["LETTRAGE"]
                })
            })
        }
        catch(error){
            console.log("error in data from FACTURE")
        }

        /////////////////////////////////////////////////////////
        // add exposant to result (exposants)
        result.push(objHubspot)
        /////////////////////////////////////////////////////////
    }

    const ws = fs.createWriteStream("./results/out.csv");
    fastcsv
      .write(result, { headers: true })
      .pipe(ws)
      /*let dataJSON = JSON.stringify(result)
      fs.writeFileSync('result2.json', dataJSON)*/
}

asyncFunc()




