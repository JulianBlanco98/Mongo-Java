package mongo_complete;

import java.util.Scanner;

public class Principal {

	public static void main(String[] args) {
		
		Mongo _mongo = new Mongo();
		
		/*
		 * ConexiÃ³n a la base de datos Mongo.
		 */
		
		System.out.println("Conectando al cliente de la base de datos NoSQL MongoDB...");
		_mongo.connect();
		
		/*
		 * CreaciÃ³n de la base de datos ejemplo
		 */
		
		_mongo.Connect_Collection("mg_julbla_2022");
		/*boolean existe = _mongo.verificarColeccion(_mongo.getDb(), "datosclima");
		
		if(existe) {
			System.out.println("existe");
		}
		else {
			System.out.println("no existe");
			
		}*/
		
		
		Scanner scanner = new Scanner(System.in);
		int opcion;

		do {
			System.out.println("Menu de la Practica");
			System.out.println("----");
			System.out.println("1. Consulta 1");
			System.out.println("2. Consulta 2");
			System.out.println("3. Consulta 3");
			System.out.println("4. Salir");
			System.out.print("Ingrese una opcion: ");
			opcion = scanner.nextInt();

			switch (opcion) {
			case 1:
				System.out.println("Ha seleccionado la opción 1");
				_mongo.consulta1();			
				break;
			case 2:
				System.out.println("Ha seleccionado la opción 2");
				_mongo.consulta2();
				break;
			case 3:
				System.out.println("Ha seleccionado la opción 3");
				_mongo.consulta3();
				break;
			case 4:
				System.out.println("Saliendo del menu...");
				break;
			default:
				System.out.println("Opción inválida. Por favor, seleccione una opción válida.");
				break;
			}
			System.out.println();
		} while (opcion != 4);
		
		_mongo.closeConnection();
		
	}

}
