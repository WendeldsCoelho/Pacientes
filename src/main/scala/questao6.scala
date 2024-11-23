//import org.apache.spark.sql.DataFrame
//import org.apache.spark.sql.functions._
//import plotly._
//import plotly.element._
//import plotly.layout._
//import plotly.Plotly
//
//object questao6 {
//  def run(dfRenomeado: DataFrame): Unit = {
//    dfRenomeado
//      .filter(row => row.getAs[String]("diagnostico") == "Hipertensão")
//      .select("atendimento", "nomePaciente", "tratamento", "Custo do tratamento")
//      //.show(false)
//
//    val hipertensao = dfRenomeado
//      .filter(col("diagnostico") === "Hipertensão")
//      .select("atendimento", "nomePaciente", "tratamento", "Custo do tratamento")
//      .collect()
//
//    val tabela = Table(
//      header = Header(
//        values = Seq("ID Atendimento", "Paciente", "Tratamento", "Custo"),
//        align = Align.Center
//      ),
//      cells = Cells(
//        values = hipertensao.map(r => Seq(
//          r.getString(0), r.getString(1), r.getString(2), r.getDouble(3).toString
//        )),
//        align = Align.Center
//      )
//    )
//
//    Plotly.plot(
//      path = "tabela_hipertensao.html",
//      traces = Seq(tabela)
//    )
//  }
//}