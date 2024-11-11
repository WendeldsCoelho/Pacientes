import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object questao1 {
  def run(dfRenomeado: DataFrame): Unit = {
    // Liste todos os atendimentos de pacientes com mais de 60 anos que receberam tratamento "Fisioterapia".
    val resultado = dfRenomeado
      .filter(col("idade") > 60 && col("tratamento") === "Fisioterapia")
      .select("atendimento", "nomePaciente", "diagnostico")

    // Exibindo o resultado completo (sem truncamento)
    resultado.show(false)
  }
}