import org.apache.spark.sql.SparkSession

object  Main extends App {
  // Criação do SparkSession
  val spark = SparkSession.builder()
    .appName("Pacientes")
    .master("local")
    .getOrCreate()

  val dfAtendimentos = spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("paciente_1000.csv")

  // Renomeando as colunas para facilitar o acesso
  val dfRenomeado = dfAtendimentos
    .withColumnRenamed("ID Atendimento", "atendimento")
    .withColumnRenamed("Paciente", "nomePaciente")
    .withColumnRenamed("Diagnóstico", "diagnostico")
    .withColumnRenamed("Tratamento", "tratamento")
    .withColumnRenamed("Idade", "idade")
    .withColumnRenamed("Médico responsável", "medico")
    .withColumnRenamed("Duração do tratamento (dias)", "duracao")
    .withColumnRenamed("Data de início", "dataInicio")
    .withColumnRenamed("Custo do tratamento", "custoTratamento")


  //Chamando a análise da sua escolha.
    //questao1.run(dfRenomeado)
    //questao2.run(dfRenomeado)
    //questao3.run(dfRenomeado)
    //questao4.run(dfRenomeado)
    //questao5.run(dfRenomeado)
    //questao6.run(dfRenomeado)
    //questao7.run(dfRenomeado)
    //questao8.run(dfRenomeado)
    //questao9.run(dfRenomeado)
    //questao10.run(dfRenomeado)

  // Parando o SparkSession
  spark.stop()
}