package mongo_complete;

import java.text.DateFormat;
import java.text.Normalizer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.Map;
import java.util.Scanner;
import java.util.TreeMap;

import com.mongodb.MongoClient;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.DistinctIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

import org.bson.Document;

import static java.util.Arrays.asList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.mongodb.Block;

public class Mongo {

	private MongoClient mongoClient;
	private MongoDatabase db;

	public Mongo() {

	}

	/*
	 * ConexiÃ³n al servidor Mongo.
	 */
	public void connect() {
		this.setMongoClient(new MongoClient("localhost", 27017));
	}

	/*
	 * ConexiÃ³n a la base de datos. Si no existe la crea.
	 */
	public void Connect_Collection(String database) {
		setDb(mongoClient.getDatabase(database));
	}

	/*
	 * Crear colecciÃ³n, si no existe la crea
	 */
	public void crear_coleccion(String coleccion) {
		getDb().getCollection(coleccion);
	}

	/*
	 * Eliminar colecciÃ³n
	 */
	public void eliminar_coleccion(String coleccion) {
		getDb().getCollection(coleccion).drop();
	}
	
	public boolean verificarColeccion(MongoDatabase database, String nombreColeccion) {
        for (String nombre : database.listCollectionNames()) {
            if (nombre.equals(nombreColeccion)) {
                return true;
            }
        }
        return false;
    }
	
	public void probarConexion() {
		
		// Verificar la conexión
        try {
            // Intentar obtener los nombres de las colecciones
            db.listCollectionNames();

            // Si no se produce ninguna excepción, la conexión se ha establecido correctamente
            System.out.println("Conexión exitosa a la base de datos");
        } catch (Exception e) {
            // Si se produce una excepción, hubo un error en la conexión
            System.out.println("Error al conectar a la base de datos: " + e.getMessage());
        }
        

	}

	public void consulta1() {
		
		mongoClient.getDatabase("mg_julbla_2022");
		probarConexion();
		System.out.println(
				"--------------------------------------------------------------------------------------------------------------------\n");

		System.out.println("Cuáles son los lugares más o menos contaminados.");
		System.out.println(
				"\tListando los valores correspondientes a: fecha, hora, nombre del contaminante, valor del contaminante, dirección, longitud y latitud.\n");

		System.out.println(
				"--------------------------------------------------------------------------------------------------------------------\n");

		// 1. Pedir contaminante

		Scanner sc = new Scanner(System.in);
		System.out.print("Introduce el nombre del contaminante: ");
		String nombreContaminante = sc.nextLine();

		// 2. Pasar a mayúsuclas y borrar los espacios del nombre introducido

		nombreContaminante = convertirMayusculas(nombreContaminante);
		nombreContaminante = eliminarAcentos(nombreContaminante);
		nombreContaminante = eliminarEspacios(nombreContaminante);

		System.out.print("Nombre contaminante corregido: " + nombreContaminante + "\n");

		// 3. Buscar a qué magnitud esta asociado el nombre del Contaminante
		// introducido. Si no está, acaba la función

		int magnitud = getMagnitud(nombreContaminante);

		if (magnitud == -1) {
			System.out.println("El nombre del Contaminante introducido no existe");
		} else {

			// 4. Buscar el máximo
			
			System.out.println("Magnitud pasada a numero: "+magnitud);
			MongoCollection<Document> collection = db.getCollection("datosclima");
			List<Float> maximos = new LinkedList<Float>();
			
			// Realizar la consulta para cada día
	        for (int i = 0; i < 24; i++) {
	            String dia = String.format("H%02d", i + 1);
	            String hora = String.format("%02d", i + 1);
	            

	            // Crear el filtro de la consulta
	            Document filter = new Document("MAGNITUD", magnitud)
	                    .append("V" + hora, "V");

	            // Crear la proyección para obtener el máximo
	            Document projection = new Document(dia, 1)
	                    .append("_id", 0);

	            // Realizar la consulta
	            Document result = collection.find(filter)
	                    .projection(projection)
	                    .sort(new Document(dia, -1))
	                    .limit(1)
	                    .first();

	            // Obtener el máximo y agregarlo a la lista
	            if (result != null) {
	            	//System.out.println(result);
	            	Object maximoObject = result.get(dia);
	            	if (maximoObject instanceof Integer) {
	            	    Integer maximoInteger = (Integer) maximoObject;
	            	    float maximo = maximoInteger.floatValue();
	            	    maximos.add(maximo);
	            	} else if (maximoObject instanceof Double) {
	            	    Double maximoDouble = (Double) maximoObject;
	            	    float maximo = maximoDouble.floatValue();
	            	    maximos.add(maximo);
	            	}
	            }
	        }

	        System.out.println("Tamaño de la lista: "+maximos.size());
	        System.out.println("Lista de los valores máximos: ");
	        // Imprimir los máximos obtenidos
	        for (Float maximo : maximos) {
	            System.out.print(maximo+", ");
	        }
	        
	        System.out.println();
	        
	        
	        int posicionMax = 0;
			if (maximos.isEmpty()) {
				System.out.println("La lista está vacía");

			}
			float maximo = ((LinkedList<Float>) maximos).getFirst(); // primer elemento como máximo inicial
			for (int i = 0; i < maximos.size(); i++) {
				float numero = maximos.get(i);
				if (numero > maximo) {
					maximo = numero;
					posicionMax = i;
				}
			}
			
			System.out.println("Máximo: "+maximo+" en la hora: "+(posicionMax+1));	
			// 5. Buscar el mínimo
			
			List<Float> minimos = new LinkedList<Float>();
			
			// Realizar la consulta para cada día
			for (int i = 0; i < 24; i++) {
				String dia = String.format("H%02d", i + 1);
				String hora = String.format("%02d", i + 1);
				
				// Crear el filtro de la consulta
				Document filter = new Document("MAGNITUD", magnitud)
						.append("V" + hora, "V");
				
				// Crear la proyección para obtener el mínimo
				Document projection = new Document(dia, 1)
						.append("_id", 0);
				
				// Realizar la consulta
				Document result = collection.find(filter)
						.projection(projection)
						.sort(new Document(dia, 1))
						.limit(1)
						.first();
				
				// Obtener el mínimo y agregarlo a la lista
				if (result != null) {
					Object minimoObject = result.get(dia);
					if (minimoObject instanceof Integer) {
						Integer minimoInteger = (Integer) minimoObject;
						float minimo = minimoInteger.floatValue();
						minimos.add(minimo);
					} else if (minimoObject instanceof Double) {
						Double minimoDouble = (Double) minimoObject;
						float minimo = minimoDouble.floatValue();
						minimos.add(minimo);
					}
				}
			}
			
			System.out.println("Tamaño de la lista: " + minimos.size());
			System.out.print("Lista de los valores mínimos:");
			// Imprimir los mínimos obtenidos
			for (Float minimo : minimos) {
				System.out.print(minimo+", ");
			}
			
			System.out.println();
	        
	        
	        int posicionMin = 0;
			if (minimos.isEmpty()) {
				System.out.println("La lista está vacía");

			}
			float minimo = ((LinkedList<Float>) minimos).getFirst(); // primer elemento como máximo inicial
			for (int i = 0; i < minimos.size(); i++) {
				float numero = minimos.get(i);
				if (numero < minimo) {
					minimo = numero;
					posicionMin = i;
				}
			}
			
			System.out.println("Mínimo: "+minimo+" en la hora: "+(posicionMin+1));	
			
			//6. Hacer la consulta para seleccionar el máximo y el mínimo y devolverlo por pantalla
			String max = String.format("%02d", (posicionMax + 1));
			String min = String.format("%02d", (posicionMin + 1));
			double max2 = (double) maximo;
			double min2 = (double) minimo;
			
			consultarValorExtremo(magnitud, max, max2, collection);
			consultarValorExtremo(magnitud, min, min2, collection);
	
		}//fin del else
		
		
	}
	
	public void consulta2() {
		
		System.out.println(
				"--------------------------------------------------------------------------------------------------------------------\n");

		System.out.println("Cuáles son los lugares cuya contaminación supera o es inferior a ciertos valores");
		System.out.println(
				"\tEn la consulta 2 se piden lugares (estaciones) donde la contaminación sea mayor a un valor introducido por el usuario, independientemente del contaminante.\n");

		System.out.println(
				"--------------------------------------------------------------------------------------------------------------------\n");

		//1. Introducir el valor para ver los datos mayores a este valor. Tiene que ser double, si no
		//no lo detecta el lenguaje de mongo db.
		
		Scanner sc = new Scanner(System.in);
		System.out.print("Introduce el valor: ");
		double valor = sc.nextDouble();
		
		while (valor < 0) {			
			System.out.print("Error. Introduce un numero positivo: ");
			valor = sc.nextDouble();
		}	
		
		valor = get2Digitos(valor);
		//System.out.println("Número: "+valor);
		
		//2. Buscar todos los valores mayores del valor introducido y hacer la media de ellos
		
		MongoCollection<Document> collection = db.getCollection("datosclima");
		HashMap<Integer, List<Double>> medias = new HashMap<Integer, List<Double>>();
		
		for (int i=0;i<24;i++) {
			String dia = String.format("%02d", i + 1);
			// Construir el filtro de la consulta
			Document filter = new Document("H" + dia, new Document("$gt", valor))
		            .append("V" + dia, "V");
			
			
			// Construir la agrupación
		    Document group = new Document("_id", "$ESTACION")
		            .append("MEDIACONTAMINANTE", new Document("$avg", "$H" + dia));
		    
		 // Proyección para incluir solo los campos deseados
		    Document projection = new Document("_id", 0)
		            .append("ESTACION", "$_id")
		            .append("MEDIACONTAMINANTE", 1);
		    
		 // Realizar la consulta de agregación
		    List<Document> pipeline = Arrays.asList(
		            new Document("$match", filter),
		            new Document("$group", group),
		            new Document("$project", projection)
		    );
		    
		    AggregateIterable<Document> result = collection.aggregate(pipeline);

		    for (Document document : result) {
		        double mediaContaminate = document.getDouble("MEDIACONTAMINANTE");
		        int estacion = document.getInteger("ESTACION");
		        agregarValores(medias, estacion, mediaContaminate);
		    }

			
		}
		
		//3. Una vez teniendo todos los valores medios de cada contaminante en cada estación, hacer la media para cada estación de los valores medios
		System.out.println("ESTOS SON LAS MEDIAS DE LOS LUGARES QUE SUPERAN EL NUMERO: " + valor + "\n");
		System.out.printf("%-30s%-30s%n", "Estacion", "Media");
		System.out.println();

		for (Map.Entry<Integer, List<Double>> entry : medias.entrySet()) {
		    int clave = entry.getKey();
		    List<Double> valores = entry.getValue();
		    float suma = 0.0f;
		    for (double dato : valores) {
		        suma += dato;
		    }
		    float media = suma / valores.size();
		    System.out.printf("%-30s%-30s%n", obtenerValorColumnaDerecha(clave), media);
		}
		

	}
	
	public void consulta3() {
		
		System.out.println(
				"--------------------------------------------------------------------------------------------------------------------\n");

		System.out.println("En la consulta 3, qué contaminación hay en cada zona");
		System.out.println(
				"\tSería listar los valores máximos (por agrupar) de los contaminantes indicados de cada zona. Entender cada zona como punto de muestreo del CSV."
				+ "buscar los 4 máximos valores de los contaminantes (SO2, CO, NO2, PM2.5) (magnitud) de cada estacion\n");

		System.out.println(
				"--------------------------------------------------------------------------------------------------------------------\n");
		
		// 1: Almacenar cada estación, los valores máximos de los 4 contaminantes

		MongoCollection<Document> collection = db.getCollection("datosclima");
		HashSet<Integer> e1 = new HashSet<Integer>(); // HASHSET para guardar las estaciones que contengan cada contaminante
		HashSet<Integer> e2 = new HashSet<Integer>();
		HashSet<Integer> e3 = new HashSet<Integer>();
		HashSet<Integer> e4 = new HashSet<Integer>();
		TreeMap<Integer, List<Double>> compuesto1 = new TreeMap<Integer, List<Double>>();// TREESET para almacenar los valores máximos de cada estación de cada contaminante
		TreeMap<Integer, List<Double>> compuesto2 = new TreeMap<Integer, List<Double>>();
		TreeMap<Integer, List<Double>> compuesto3 = new TreeMap<Integer, List<Double>>();
		TreeMap<Integer, List<Double>> compuesto4 = new TreeMap<Integer, List<Double>>();
		
		// Segundo: Rellenar cada TreeSet con todos los valores máximos de cada hora de
		// cada contaminantes en las diferentes estaciones.
		// A su vez, rellenar cada HashSet para saber en que estaciones está el
		// contaminante	
		
		for (int i = 0; i < 24; i++) {
		    String dia = String.format("%02d", i + 1);

		    // Consulta para magnitud = 1
		    Document filter1 = new Document("MAGNITUD", 1)
		            .append("V" + dia, "V");

		    Document group1 = new Document("_id", "$ESTACION")
		            .append("MAXIMO", new Document("$max", "$H" + dia));

		    Document projection1 = new Document("_id", 0)
		            .append("ESTACION", "$_id")
		            .append("MAXIMO", 1);

		    List<Document> pipeline1 = Arrays.asList(
		            new Document("$match", filter1),
		            new Document("$group", group1),
		            new Document("$project", projection1)
		    );

		    AggregateIterable<Document> result1 = collection.aggregate(pipeline1);
		    for (Document document : result1) {
		    	
		    	Object maximoObj = document.get("MAXIMO");
		    	double maximo = Double.parseDouble(maximoObj.toString());
		        int estacion = document.getInteger("ESTACION");
		        e1.add(estacion);
		        agregarValores(compuesto1, estacion, maximo);
		    }

		    // Consulta para magnitud = 6 
		    Document filter2 = new Document("MAGNITUD", 6)
		            .append("V" + dia, "V");

		    Document group2 = new Document("_id", "$ESTACION")
		            .append("MAXIMO", new Document("$max", "$H" + dia));

		    Document projection2 = new Document("_id", 0)
		            .append("ESTACION", "$_id")
		            .append("MAXIMO", 1);

		    List<Document> pipeline2 = Arrays.asList(
		            new Document("$match", filter2),
		            new Document("$group", group2),
		            new Document("$project", projection2)
		    );

		    AggregateIterable<Document> result2 = collection.aggregate(pipeline2);
		    for (Document document : result2) {
		    	
		    	Object maximoObj = document.get("MAXIMO");
		    	double maximo = Double.parseDouble(maximoObj.toString());
		        int estacion = document.getInteger("ESTACION");
		        e2.add(estacion);
		        agregarValores(compuesto2, estacion, maximo);
		    }
		    // Consulta para magnitud = 8
		    Document filter3 = new Document("MAGNITUD", 8)
		    		.append("V" + dia, "V");
		    
		    Document group3 = new Document("_id", "$ESTACION")
		    		.append("MAXIMO", new Document("$max", "$H" + dia));
		    
		    Document projection3 = new Document("_id", 0)
		    		.append("ESTACION", "$_id")
		    		.append("MAXIMO", 1);
		    
		    List<Document> pipeline3 = Arrays.asList(
		    		new Document("$match", filter3),
		    		new Document("$group", group3),
		    		new Document("$project", projection3)
		    		);
		    
		    AggregateIterable<Document> result3 = collection.aggregate(pipeline3);
		    for (Document document : result3) {
		    	
		    	Object maximoObj = document.get("MAXIMO");
		    	double maximo = Double.parseDouble(maximoObj.toString());
		    	int estacion = document.getInteger("ESTACION");
		    	e3.add(estacion);
		    	agregarValores(compuesto3, estacion, maximo);
		    }
		    // Consulta para magnitud = 9
		    Document filter4 = new Document("MAGNITUD", 9)
		    		.append("V" + dia, "V");
		    
		    Document group4 = new Document("_id", "$ESTACION")
		    		.append("MAXIMO", new Document("$max", "$H" + dia));
		    
		    Document projection4 = new Document("_id", 0)
		    		.append("ESTACION", "$_id")
		    		.append("MAXIMO", 1);
		    
		    List<Document> pipeline4 = Arrays.asList(
		    		new Document("$match", filter4),
		    		new Document("$group", group4),
		    		new Document("$project", projection4)
		    		);
		    
		    AggregateIterable<Document> result4 = collection.aggregate(pipeline4);
		    for (Document document : result4) {
		    	
		    	Object maximoObj = document.get("MAXIMO");
		    	double maximo = Double.parseDouble(maximoObj.toString());
		    	int estacion = document.getInteger("ESTACION");
		    	e4.add(estacion);
		    	agregarValores(compuesto4, estacion, maximo);
		    }
		}
		//Tercero: ArrayList para almacenar todas las estaciones del CSV
		List<Integer> estaciones = new ArrayList<Integer>();
		DistinctIterable<Integer> distinctEstaciones = collection.distinct("ESTACION", Integer.class);
		for (Integer estacion : distinctEstaciones) {
			estaciones.add(estacion);
		}
		mostrarEstaciones(estaciones);
		
		//Cuarto: Listas para guardar los máximos, las horas de cada contaminante y las estaciones donde existe cada contaminante
		//4.1
		LinkedList<Double> maximos1 = new LinkedList<Double>();
		LinkedList<Double> posiciones1 = new LinkedList<Double>();
		maximos1 = obtenerMaximo(compuesto1);
		posiciones1 = obtenerPosicion(compuesto1);
		List<Integer> e1ordenadas = new LinkedList<Integer>(e1);
		Collections.sort(e1ordenadas);
		//4.2
		LinkedList<Double> maximos2 = new LinkedList<Double>();
		LinkedList<Double> posiciones2 = new LinkedList<Double>();
		maximos2 = obtenerMaximo(compuesto2);
		posiciones2 = obtenerPosicion(compuesto2);
		List<Integer> e2ordenadas = new LinkedList<Integer>(e2);
		Collections.sort(e2ordenadas);
		//4.3
		LinkedList<Double> maximos3 = new LinkedList<Double>();
		LinkedList<Double> posiciones3 = new LinkedList<Double>();
		maximos3 = obtenerMaximo(compuesto3);
		posiciones3 = obtenerPosicion(compuesto3);
		List<Integer> e3ordenadas = new LinkedList<Integer>(e3);
		Collections.sort(e3ordenadas);
		//4.4
		LinkedList<Double> maximos4 = new LinkedList<Double>();
		LinkedList<Double> posiciones4 = new LinkedList<Double>();
		maximos4 = obtenerMaximo(compuesto4);
		posiciones4 = obtenerPosicion(compuesto4);
		List<Integer> e4ordenadas = new LinkedList<Integer>(e4);
		Collections.sort(e4ordenadas);
		
		System.out.println(String.format("%-5s\t%-5s\t%-5s\t%-16s\t%-16s\t%-16s\t%-16s\t%-16s\t%-16s\t%-16s",
                "ano", "mes", "dia", "estacion", "longitud", "latitud", "Direccion", "Valor",
                "Nombre contaminante", "Hora"));
		System.out.println("-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------");
		
		
		boolean encontrado1 = false;
		boolean encontrado2 = false;
		boolean encontrado3 = false;
		boolean encontrado4 = false;
		int cont1 = 0;
		int cont2 = 0;
		int cont3 = 0;
		int cont4 = 0;

		// 5. Recorrer cada estación. Luego, recorrer cada contaminante en esa estación. Si tiene máximo, se muestra por pantalla
		
		try {
			
			//5.1 Recorro cada estación
			for (int i = 0; i < estaciones.size(); i++) {
				encontrado1 = false;
		        encontrado2 = false;
		        encontrado3 = false;
		        encontrado4 = false;
		     // Si la estación del primer contaminante coincide con la estación por la que se va buscando
		        if (estaciones.get(i) == e1ordenadas.get(cont1)) {
		            for (int j = 0; j < maximos1.size(); j++) {
		                j = cont1;
		                String dia = String.format("%02d", Math.round(posiciones1.get(j)));
		                double m = maximos1.get(j);

		                // Buscar el máximo ya encontrado antes
		                Document query1 = new Document("H" + dia, m)
		                        .append("MAGNITUD", 1)
		                        .append("ESTACION", estaciones.get(i));

		                MongoCursor<Document> cursor1 = collection.find(query1).iterator();
		                while (cursor1.hasNext() && !encontrado1) {
		                    Document document = cursor1.next();
		                    Object maximoObj = document.get("H" + dia);
		    		    	double maximo = Double.parseDouble(maximoObj.toString());
		                    System.out.printf("%-5s\t%-5s\t%-5s\t%-16s\t%-16s\t%-16s\t%-16s\t%-16s\t%-16s\t%-16s%n",
		                            document.getInteger("ANO"), document.getInteger("MES"), document.getInteger("DIA"),
		                            document.getInteger("ESTACION"), document.getInteger("LONGITUD"),
		                            document.getInteger("LATITUD"), document.getString("DIRECCION)"), maximo, getAbreviatura(document.getInteger("MAGNITUD")), "H" + dia);
		                    encontrado1 = true;
		                }

		                cursor1.close();

		                if (encontrado1) {
		                    j = maximos1.size() - 1;
		                    cont1++;
		                }
		            }
		        	
		        }

				// Si la estación del segundo contaminante coincide con la estación por la que
				// se va buscando
				if (estaciones.get(i) == e2ordenadas.get(cont2)) {
					for (int j = 0; j < maximos2.size(); j++) {
						j = cont2;
						String dia = String.format("%02d", Math.round(posiciones2.get(j)));
						double m = maximos2.get(j);

						// Buscar el máximo ya encontrado antes
						Document query2 = new Document("H" + dia, m).append("MAGNITUD", 6).append("ESTACION",
								estaciones.get(i));

						MongoCursor<Document> cursor2 = collection.find(query2).iterator();

						while (cursor2.hasNext() && !encontrado2) {
							Document document = cursor2.next();
							Object maximoObj = document.get("H" + dia);
							double maximo = Double.parseDouble(maximoObj.toString());
							System.out.printf("%-5s\t%-5s\t%-5s\t%-16s\t%-16s\t%-16s\t%-16s\t%-16s\t%-16s\t%-16s%n",
									document.getInteger("ANO"), document.getInteger("MES"), document.getInteger("DIA"),
									document.getInteger("ESTACION"), document.getInteger("LONGITUD"),
									document.getInteger("LATITUD"), document.getString("DIRECCION)"), maximo,
									getAbreviatura(document.getInteger("MAGNITUD")), "H" + dia);
							encontrado2 = true;
						}

						cursor2.close();

						if (encontrado2) {
							j = maximos2.size() - 1;
							cont2++;
						}
					}
				}//fin if magnitud 6
				
				// Si la estación del tercer contaminante coincide con la estación por la que se va buscando
			    if (estaciones.get(i) == e3ordenadas.get(cont3)) {
			        for (int j = 0; j < maximos3.size(); j++) {
			            j = cont3;
			            String dia = String.format("%02d", Math.round(posiciones3.get(j)));
			            double m = maximos3.get(j);

			            // Buscar el máximo ya encontrado antes
			            Document query3 = new Document("H" + dia, m)
			                    .append("MAGNITUD", 8)
			                    .append("ESTACION", estaciones.get(i));

			            MongoCursor<Document> cursor3 = collection.find(query3).iterator();

			            while (cursor3.hasNext() && !encontrado3) {
			                Document document = cursor3.next();
			                Object maximoObj = document.get("H" + dia);
					    	double maximo = Double.parseDouble(maximoObj.toString());
			                System.out.printf("%-5s\t%-5s\t%-5s\t%-16s\t%-16s\t%-16s\t%-16s\t%-16s\t%-16s\t%-16s%n",
			                        document.getInteger("ANO"), document.getInteger("MES"), document.getInteger("DIA"),
			                        document.getInteger("ESTACION"), document.getInteger("LONGITUD"),
			                        document.getInteger("LATITUD"), document.getString("DIRECCION)"), maximo, getAbreviatura(document.getInteger("MAGNITUD")), "H" + dia);
			                encontrado3 = true;
			            }

			            cursor3.close();

			            if (encontrado3) {
			                j = maximos3.size() - 1;
			                cont3++;
			            }
			        }
			    }
			    
			    // Si la estación del cuarto contaminante coincide con la estación por la que se va buscando
			    if (estaciones.get(i) == e4ordenadas.get(cont4)) {
			        for (int j = 0; j < maximos4.size(); j++) {
			            j = cont4;
			            String dia = String.format("%02d", Math.round(posiciones4.get(j)));
			            double m = maximos4.get(j);

			            // Buscar el máximo ya encontrado antes
			            Document query4 = new Document("H" + dia, m)
			                    .append("MAGNITUD", 9)
			                    .append("ESTACION", estaciones.get(i));

			            MongoCursor<Document> cursor4 = collection.find(query4).iterator();

			            while (cursor4.hasNext() && !encontrado4) {
			                Document document = cursor4.next();
			                Object maximoObj = document.get("H" + dia);
					    	double maximo = Double.parseDouble(maximoObj.toString());
			                System.out.printf("%-5s\t%-5s\t%-5s\t%-16s\t%-16s\t%-16s\t%-16s\t%-16s\t%-16s\t%-16s%n",
			                        document.getInteger("ANO"), document.getInteger("MES"), document.getInteger("DIA"),
			                        document.getInteger("ESTACION"), document.getInteger("LONGITUD"),
			                        document.getInteger("LATITUD"), document.getString("DIRECCION)"), maximo, getAbreviatura(document.getInteger("MAGNITUD")), "H" + dia);
			                encontrado4 = true;
			            }

			            cursor4.close();

			            if (encontrado4) {
			                j = maximos4.size() - 1;
			                cont4++;
			            }
			        }
			    }

		        
				
				
				
			}//fin for
			
			
		}
		catch(IndexOutOfBoundsException e) {
			
		}
			
	}
		
	private String getAbreviatura(int magnitud) {
		if (magnitud == 1) {
	        return "S02";
	    } else if (magnitud == 6) {
	        return "C0";
	    } else if (magnitud == 8) {
	        return "N02";
	    } else if (magnitud == 9) {
	        return "PM2.5";
	    } else {
	        return "";
	    }
	}

	private LinkedList<Double> obtenerPosicion(TreeMap<Integer, List<Double>> treeMap) {

		LinkedList<Double> aux = new LinkedList<Double>();

		for (Map.Entry<Integer, List<Double>> entry : treeMap.entrySet()) {
			int posicion = -1;
			double maximo = Double.MIN_VALUE;
			List<Double> valores = entry.getValue();
			for (int i = 0; i < valores.size(); i++) {
				double valor = valores.get(i);
				if (valor > maximo) {
					maximo = valor;
					posicion = i;
				}
			}
			aux.add((posicion + 1.0));
		}
		return aux;
	}


	private LinkedList<Double> obtenerMaximo(TreeMap<Integer, List<Double>> treeMap) {
        
	    LinkedList<Double> aux = new LinkedList<Double>();

	    for (Map.Entry<Integer, List<Double>> entry : treeMap.entrySet()) {
	        int posicion = -1;
	        double maximo = Double.MIN_VALUE;
	        List<Double> valores = entry.getValue();
	        for (int i = 0; i < valores.size(); i++) {
	            double valor = valores.get(i);
	            if (valor > maximo) {
	                maximo = valor;
	                posicion = i;
	            }
	        }
	        aux.add(maximo);
	    }
	    return aux;
	}

	private void mostrarEstaciones(List<Integer> estaciones) {
		// TODO Auto-generated method stub
		System.out.print("Estaciones: ");
		Iterator it = estaciones.iterator();
		while(it.hasNext()) {
			
			System.out.print(it.next()+", ");
		}
		System.out.println();
	}

	private String obtenerValorColumnaDerecha(int numeroColumnaIzquierda) {
		
		switch (numeroColumnaIzquierda) {
        case 55:
            return "Urb. Embajada";
        case 50:
            return "Plaza de Castilla";
        case 49:
            return "Parque del Retiro";
        case 60:
            return "Tres Olivos";
        case 16:
            return "Arturo Soria";
        case 11:
            return "Ramón y Cajal";
        case 8:
            return "Escuelas Aguirre";
        case 4:
            return "Plaza de España";
        case 18:
            return "Farolillos";
        case 47:
            return "Mendez Alvaro";
        case 54:
            return "Ensanche de Vallecas";
        case 58:
            return "El Pardo";
        case 27:
            return "Barajas Pueblo";
        case 59:
            return "Juan Carlos I";
        case 36:
            return "Moratalaz";
        case 40:
            return "Vallecas";
        case 38:
            return "Cuatro Caminos";
        case 57:
            return "Sanchinarro";
        case 39:
            return "Barrio del Pilar";
        case 56:
            return "Plaza Elíptica";
        case 17:
            return "Villaverde";
        case 35:
            return "Plaza del Carmen";
        case 48:
            return "Castellana";
        case 24:
            return "Casa de Campo";
        default:
            return "Valor no encontrado";
		}
    
	}
	private static void agregarValores(TreeMap<Integer, List<Double>> medias, int estacion, double valor) {
        List<Double> valores = medias.getOrDefault(estacion, new ArrayList<Double>());
        valores.add(valor);
        medias.put(estacion, valores);
    }

	private void agregarValores(HashMap<Integer, List<Double>> medias, int estacion, double mediaContaminate) {
		
		
		List<Double> valores = medias.getOrDefault(estacion, new ArrayList<Double>());
		valores.add(mediaContaminate);
		medias.put(estacion, valores);
		
	}

	public void consultarValorExtremo(int magnitud, String hora, double valor, MongoCollection<Document> col) {
	    
		
		System.out.println("\nParámetros");
		System.out.println("Magnitud: "+magnitud+" , Hora: H"+hora+", valorC: "+valor);
		String h ="H"+hora;
		
		valor = get2Digitos(valor);

	    // Crear el filtro de la consulta
	    Document filter = new Document("MAGNITUD", magnitud)
	            .append(h, valor);

	    // Crear la proyección
	    Document projection = new Document("ANO", 1)
	            .append("MES", 1)
	            .append("DIA", 1)
	            .append("H" + hora, 1)
	            .append("NOMBRE_CONTAMINANTE", 1)
	            .append("DIRECCION)", 1)
	            .append("LONGITUD", 1)
	            .append("LATITUD", 1)
	            .append("_id", 0);

	    // Realizar la consulta
	    Document result = col.find(filter)
	            .projection(projection)
	            .limit(1)
	            .first();
	            
	

	    if (result != null) {
	    	System.out.println(String.format("%-4s\t%-4s\t%-4s\t%-10s\t%-4s\t%-4s\t%-8s\t%-8s", "ano", "mes", "dia",
	    			"h" + hora + "", "nombre_contaminante", "direccion", "longitud", "latitud"));
	    	System.out.println("------------------------------------------------------------------------------------------------------------------");
	        int ano = result.getInteger("ANO");
	        int mes = result.getInteger("MES");
	        int dia = result.getInteger("DIA");
	        Object valorObj = result.get("H" + hora);
	        double valorExtremo = Double.parseDouble(valorObj.toString());
	        String nombreContaminante = result.getString("NOMBRE_CONTAMINANTE");
	        String direccion = result.getString("DIRECCION)");
	        int longitud = result.getInteger("LONGITUD");
	        int latitud = result.getInteger("LATITUD");
	        System.out.println(String.format("%-4d\t%-4d\t%-4d\t%-10f\t%-4s\t%-4s\t%-8d\t%-8d", ano, mes, dia, valorExtremo, nombreContaminante, direccion, longitud, latitud));
	    } else {
	        System.out.println("No se encontró ningún valor " + valor + " para la magnitud " + magnitud);
	    }
	}
	
	


	private double get2Digitos(double valor) {
		
		valor = Math.round(valor * 100.0) / 100.0;
		return valor;
	}

	private int getMagnitud(String contaminante) {
		if (contaminante.equals("DIOXIDODEAZUFRE")) {
			return 1;
		} else if (contaminante.equals("MONOXIDODECARBONO")) {
			return 6;
		} else if (contaminante.equals("MONOXIDODENITROGENO")) {
			return 7;
		} else if (contaminante.equals("DIOXIDODENITROGENO")) {
			return 8;
		} else if (contaminante.equals("PARTICULAS<2.5ΜM")) {
			return 9;
		} else if (contaminante.equals("PARTICULAS<10ΜM")) {
			return 10;
		} else if (contaminante.equals("OXIDOSDENITROGENO")) {
			return 12;
		} else if (contaminante.equals("OZONO")) {
			return 14;
		} else if (contaminante.equals("TOLUENO")) {
			return 20;
		} else if (contaminante.equals("BENCENO")) {
			return 30;
		} else if (contaminante.equals("ETILBENCENO")) {
			return 35;
		} else if (contaminante.equals("METAXILENO")) {
			return 37;
		} else if (contaminante.equals("PARAXILENO")) {
			return 38;
		} else if (contaminante.equals("ORTOXILENO")) {
			return 39;
		} else if (contaminante.equals("HIDROCARBUROSTOTALES")) {
			return 42;
		} else if (contaminante.equals("METANO")) {
			return 43;
		} else if (contaminante.equals("HIDROCARBUROSNOMETANICOS")) {
			return 44;
		} else {
			return -1; // Valor por defecto en caso de no encontrar el contaminante
		}
	}

	private String convertirMayusculas(String texto) {
		return texto.toUpperCase();
	}

	private String eliminarEspacios(String texto) {
		return texto.replaceAll("\\s", "");
	}

	private String eliminarAcentos(String texto) {
		String textoSinAcentos = Normalizer.normalize(texto, Normalizer.Form.NFD);
		textoSinAcentos = textoSinAcentos.replaceAll("\\p{M}", "");
		return textoSinAcentos;
	}

	public MongoClient getMongoClient() {
		return mongoClient;
	}

	public void setMongoClient(MongoClient mongoClient) {
		this.mongoClient = mongoClient;
	}

	public MongoDatabase getDb() {
		return db;
	}

	public void setDb(MongoDatabase db) {
		this.db = db;
	}
	public void closeConnection() {
		
		System.out.println("Conexión cerrada");
		this.mongoClient.close();
	}

}
