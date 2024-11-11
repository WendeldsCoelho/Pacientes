import org.apache.spark.sql.SparkSession

object Main extends App {
  // Criação do SparkSession
  val spark = SparkSession.builder()
    .appName("Pacientes")
    .master("local")  // Definido para rodar localmente
    .getOrCreate()

  // Carregando o CSV com a inferência de schema
  val dfAtendimentos = spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("pacientes.csv")

  // Renomeando as colunas para facilitar o acesso
  val dfRenomeado = dfAtendimentos
    .withColumnRenamed("ID Atendimento", "atendimento")
    .withColumnRenamed("Paciente", "nomePaciente")
    .withColumnRenamed("Diagnóstico", "diagnostico")
    .withColumnRenamed("Tratamento", "tratamento")
    .withColumnRenamed("Idade", "idade")
    .withColumnRenamed("Duração do tratamento (dias)", "duracao")
    .withColumnRenamed("Data de início", "dataInicio")

  // Chamando a análise da primeira questão
  //questao1.run(dfRenomeado)
  //questao2.run(dfRenomeado)
  //questao3.run(dfRenomeado)
  questao4.run(dfRenomeado)


  // Parando o SparkSession
  spark.stop()
}