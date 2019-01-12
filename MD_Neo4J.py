#!/usr/bin/env python
#Datu apstrādes sistēmas Mājas darbs #2 "Grafu datubāzes"

import os, sys
from json import dumps
from flask import Flask, g, Response, request, render_template
from pathlib import Path
from neo4j.v1 import GraphDatabase, basic_auth

app = Flask(__name__)
app.debug = True

#lokālā Neo4J datubāze
dbIp = 'localhost'
dbPort = '7687'
neo4jPass = 'lutest'
browserUrl = 'http://localhost:7474/browser'


driver = GraphDatabase.driver('bolt://'+ dbIp + ':'+ dbPort,auth=basic_auth("neo4j", neo4jPass))
url = browserUrl + '/?cmd=play&arg=MATCH (n) RETURN (n)'

def get_db():
    if not hasattr(g, 'neo4j_db'):
        g.neo4j_db = driver.session()
    return g.neo4j_db

@app.teardown_appcontext
def close_db(error):
    if hasattr(g, 'neo4j_db'):
        g.neo4j_db.close()

@app.route("/")
def get_index():
	return render_template('index.html', saturs='Kods darbībām ar datubāzi Neo4J')

#datu uzģenerēšana
@app.route("/generateData")
def generateData():
    data = Path('sampleQuery.txt').read_text()
    db = get_db()
    db.run(data)
    return render_template('index.html', saturs='Dati uzģenerēti', url=url)

#visu datu dzēšana
@app.route("/deleteData")
def deleteData():
	db = get_db()
	db.run("MATCH (n) DETACH DELETE n")
	return render_template('index.html', saturs='Dati izdzēsti', url=url)

#atskaite Nr.1
@app.route("/report1")
def report1():
    apraksts = '1.atskaite<br> Pārdošanas darījumu skaits, darījumu kopsumma pa uzņēmumiem, lielākais darījums <br> TOP10 uzņēmumi pēc darījumu kopsummas, sakārtots pēc darījumu kopsummas'
    db = get_db()
    query = """MATCH (u:uznemums)-[:pardevejs_in]-(r:rekins) 
                WITH u, count(r) AS rekinu_skaits, (round(sum(r.summa)*100))/100 AS summa, max(r.summa) AS max_darijums 
                RETURN u.title AS nosaukums, rekinu_skaits, summa, max_darijums ORDER BY summa DESC LIMIT 10"""
    query = ' '.join(query.split()) #izņemam liekās atstarpes, kuras salikām kodā, lai vieglāk lasāms kods
    results = db.run(query)
    table = '''<table class="table table-hover table-sm">
                <thead><tr>
                <th>#</th>
                <th>Nosaukums</th>
                <th>Darījumu skaits</th>
                <th>Darījumu kopsumma</th>
                <th>Lielākais darījums</th>
                </tr></thead><tbody>'''
    i=1
    for record in results:
        table += ('<tr><td>'+str(i)+'</td><td>'+record["nosaukums"]+'</td><td>'+str(record["rekinu_skaits"])
            +'</td><td>'+str(record["summa"])+'</td><td>'+str(record["max_darijums"])+'</td></tr>')
        i += 1
    table += '</tbody></table>'
    #aiz tabulas tiks pievienota saite, lai ērti pieprasījumu apskatītu Neo4J pārlūkā. Pirms izpildes no pieprasījuma jāizņem ":play"
    table += '<a href="'+browserUrl+'/?cmd=play&arg='+query+'" target="_blank" class="query">Atvērt atlasi pārlūkā</a>'
    return render_template('index.html', saturs=apraksts+table, url=url)

#atskaite Nr.2
@app.route("/report2")
def report2():
    apraksts = '2.atskaite<br> Savstarpējie darījumi ar vieniem un tiem pašiem uzņēmumiem <br>Darījuma partneri, savstarpējo darījumu skaits, summa, periods<br>Minimālais darījumu skaits 4'
    db = get_db()
    query = """MATCH (pardevejs:uznemums)-[:pardevejs_in]->(r:rekins)<-[:pircejs_in]-(pircejs:uznemums) 
            WITH pardevejs, pircejs, count(r) AS skaits, (round(sum(r.summa)*100))/100 AS summa,
            min(r.datums) AS datums_no, max(r.datums) AS datums_lidz
            WHERE skaits>=4
            RETURN pardevejs.title AS pardevejs, pircejs.title AS pircejs, skaits, summa, datums_no, datums_lidz
            ORDER BY skaits DESC"""
    query = ' '.join(query.split()) #izņemam liekās atstarpes, kuras salikām kodā, lai vieglāk lasāms kods
    results = db.run(query)
    table = '''<table class="table table-hover table-sm">
                <thead><tr>
                <th>#</th>
                <th>Pārdevējs</th>
                <th>Pircējs</th>
                <th>Darījumu skaits</th>
                <th>Darījumu kopsumma</th>
                <th>Periods no</th>
                <th>Periods līdz</th>
                </tr></thead><tbody>'''
    i=1
    for record in results:
        table += ('<tr><td>'+str(i)+'</td><td>'+record["pardevejs"]+'</td><td>'+record["pircejs"]
            +'</td><td>'+str(record["skaits"])+'</td><td>'+str(record["summa"])+'</td><td>'
            +record["datums_no"]+'</td><td>'+record["datums_lidz"]+'</td></tr>')
        i += 1
    table += '</tbody></table>'
    #aiz tabulas tiks pievienota saite, lai ērti pieprasījumu apskatītu Neo4J pārlūkā. Pirms izpildes no pieprasījuma jāizņem ":play"
    table += '<a href="'+browserUrl+'/?cmd=play&arg='+query+'" target="_blank" class="query">Atvērt atlasi pārlūkā</a>'
    return render_template('index.html', saturs=apraksts+table, url=url)

#atskaite Nr.3
@app.route("/report3")
def report3():
    apraksts = '3.atskaite<br>Darījumi, kur viena un tā pati fiziskā persona ir gan pircēja vadība, gan pārdevēja vadība (darījumu skaits>1)'
    db = get_db()
    query = """MATCH (p1:persona)-[:VADIBA_IN]->(pardevejs:uznemums)-[:pardevejs_in]->(r:rekins)<-[:pircejs_in]-(pircejs:uznemums)<-[:VADIBA_IN]-(p2:persona)
            WITH p1, p2, count(r) AS skaits, (round(sum(r.summa)*100))/100 AS summa, (round(avg(r.summa)*100)/100) AS videji,
            min(r.datums) AS datums_no, max(r.datums) AS datums_lidz
            WHERE p1=p2 AND skaits>1
            RETURN skaits, summa, datums_no, datums_lidz, p1.title AS vards, videji
            ORDER BY skaits DESC"""
    query = ' '.join(query.split()) #izņemam liekās atstarpes, kuras salikām kodā, lai vieglāk lasāms kods
    results = db.run(query)
    table = '''<table class="table table-hover table-sm">
                <thead><tr>
                <th>#</th>
                <th>Persona</th>
                <th>Darījumu skaits</th>
                <th>Vidējā darījuma summa</th>
                <th>Darījumu kopsumma</th>
                <th>Periods no</th>
                <th>Periods līdz</th>
                </tr></thead><tbody>'''
    i=1
    for record in results:
        table += ('<tr><td>'+str(i)+'</td><td>'+record["vards"]+'</td>'
            +'<td>'+str(record["skaits"])+'</td><td>'+str(record["videji"])+'</td><td>'+str(record["summa"])+'</td><td>'
            +record["datums_no"]+'</td><td>'+record["datums_lidz"]+'</td></tr>')
        i += 1
    table += '</tbody></table>'
    #aiz tabulas tiks pievienota saite, lai ērti pieprasījumu apskatītu Neo4J pārlūkā. Pirms izpildes no pieprasījuma jāizņem ":play"
    table += '<a href="'+browserUrl+'/?cmd=play&arg='+query+'" target="_blank" class="query">Atvērt atlasi pārlūkā</a>'
    return render_template('index.html', saturs=apraksts+table, url=url)

#atskaite Nr.4
@app.route("/report4")
def report4():
    apraksts = '4.atskaite<br>LTRK(reg_no:40003081501) biedru savstarpējie darījumi, kuru juridiskā adrese ir Rīgā'
    db = get_db()
    query = """MATCH (:uznemums {reg_no:'40003081501'})<-[:biedrs_in]-(u1:uznemums{city:'Rīga'})-[:pardevejs_in]->(r:rekins)
                <-[:pircejs_in]-(u2:uznemums{city:'Rīga'})-[:biedrs_in]->(:uznemums {reg_no:'40003081501'})
            RETURN u1.title AS pardevejs, u2.title AS pircejs, r.numurs AS rekins, r.datums AS datums, r.summa AS summa
            ORDER BY datums"""
    query = ' '.join(query.split()) #izņemam liekās atstarpes, kuras salikām kodā, lai vieglāk lasāms kods
    results = db.run(query)
    table = '''<table class="table table-hover table-sm">
                <thead><tr>
                <th>#</th>
                <th>Pārdevējs</th>
                <th>Pircējs</th>
                <th>Rēķina datums</th>
                <th>Rēķina numurs</th>
                <th>Summa</th>
                </tr></thead><tbody>'''
    i=1
    for record in results:
        table += ('<tr><td>'+str(i)+'</td><td>'+record["pardevejs"]+'</td>'
            +'<td>'+record["pircejs"]+'</td><td>'+record["datums"]+'</td><td>'+record["rekins"]+'</td><td>'
            +str(record["summa"])+'</td></tr>')
        i += 1
    table += '</tbody></table>'
    #aiz tabulas tiks pievienota saite, lai ērti pieprasījumu apskatītu Neo4J pārlūkā. Pirms izpildes no pieprasījuma jāizņem ":play"
    table += '<a href="'+browserUrl+'/?cmd=play&arg='+query+'" target="_blank" class="query">Atvērt atlasi pārlūkā</a>'
    return render_template('index.html', saturs=apraksts+table, url=url)

#atskaite Nr.5
@app.route("/report5")
def report5(returnQuery=None):
    apraksts = '5.atskaite<br>Personas "Jānis Zicāns" kontrolēto uzņēmumu darījumu grafisks attēlojums'
    db = get_db()
    query = """MATCH (p1:persona{title:'Jānis Zicāns'})-[r1]->(u1:uznemums)-[r2:pircejs_in|:pardevejs_in]->(rek:rekins)
            <-[r:pircejs_in|:pardevejs_in]-(u2:uznemums)  
            WHERE rek.summa>9000 
            RETURN p1, u1, u2, p1.title AS vards, u1.title AS nosaukums1, u1.reg_no AS reg_no1, 
                u2.title AS nosaukums2, u2.reg_no AS reg_no2, 
                rek, rek.numurs AS rekins_nr, rek.title AS rekins_txt, rek.summa AS rekins_summa, rek.datums AS rekins_datums,
                type(r1) AS rel_person, type(r) AS rel_uznemums, type(r2) AS rel_uznemums2, 
            ID(p1) AS id_persona, ID(u1) AS id_uznemums1, ID(u2) AS id_uznemums2, ID(rek) AS id_rekins"""
    query = ' '.join(query.split()) #izņemam liekās atstarpes, kuras salikām kodā, lai vieglāk lasāms kods
    if (returnQuery==1): return query
    url2 = '<a href="'+browserUrl+'/?cmd=play&arg='+query+'" target="_blank" class="query">Atvērt atlasi pārlūkā</a>'
    return render_template('graph.html', saturs=apraksts, browserUrl=url2, url=url, graph="/graph1")

#JSON datu atgriešana 5.atskaitei vizualizācijas attēlojumam
@app.route("/graph1")
def get_graph1():
    db = get_db()
    query = report5(1)
    results = db.run(query)
    nodes = []
    rels = []
    i = 0
    y = 100000
    for record in results:
        nodes.append({"id":"p1"+str(record["id_persona"]), "title": record["vards"], "name": record["vards"], "labels": "personas", "properties":{"name": record["vards"]}})
        nodes.append({"id":"u"+str(record["reg_no1"]), "title": record["nosaukums1"], "name": record["nosaukums1"], "labels": "uznemums", "properties":{"name": record["nosaukums1"]}})
        nodes.append({"id":"u"+str(record["reg_no2"]), "title": record["nosaukums2"], "name": record["nosaukums2"], "labels": "uznemums", "properties":{"name": record["nosaukums2"]}})
        nodes.append({"id":"r"+str(record["rekins_nr"]), "title": record["rekins_txt"], "name": record["rekins_txt"], "labels": "darijums", "properties":{"numurs": record["rekins_txt"], "datums": record["rekins_datums"], "summa": record["rekins_datums"]}})
        rels.append({"id":i, "startNode": "u"+str(record["reg_no1"]), "endNode": "r"+str(record["rekins_nr"]), "type": record["rel_uznemums2"], "properties":[]})
        i +=1
        rels.append({"id":i, "startNode": "p1"+str(record["id_persona"]), "endNode": "u"+str(record["reg_no1"]), "type": record["rel_person"], "properties":[]})
        i +=1
        rels.append({"id":i, "endNode": "u"+str(record["reg_no2"]), "startNode": "r"+str(record["rekins_nr"]), "type": record["rel_uznemums"], "properties":[]})
    data = dumps({"nodes": nodes, "relationships": rels})
    jsonData = '''{"results":[{
    		"columns":[],
    		"data":[{
    			"graph": 
    				'''+data+'''
    		}]
    	}]}'''
    return Response(jsonData,mimetype="application/json")



if __name__ == '__main__':
    app.run(port=8000)