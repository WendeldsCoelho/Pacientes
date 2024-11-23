//import org.apache.spark.sql.DataFrame
//import org.apache.spark.sql.functions._
//import plotly._
//import plotly.element._
//import plotly.layout._
//import plotly.Plotly
//
//object questao10 {
//  def run(dfRenomeado: DataFrame): Unit = {
//    dfRenomeado
//      .withColumn("faixa_etaria",
//        when(col("idade") <= 18, "0-18")
//          .when(col("idade").between(19, 30), "19-30")
//          .when(col("idade").between(31, 60), "31-60")
//          .otherwise("60+"))
//      .groupBy("faixa_etaria")
//      .count()
//      .orderBy("faixa_etaria")
//     // .show(false)
//
//    val faixas = dfRenomeado
//      .withColumn("faixa_etaria",
//        when(col("idade") <= 18, "0-18")
//          .when(col("idade").between(19, 30), "19-30")
//          .when(col("idade").between(31, 60), "31-60")
//          .otherwise("60+"))
//      .groupBy("faixa_etaria")
//      .count()
//      .collect()
//
//    val faixasEtarias = faixas.map(_.getString(0))
//    val quantidades = faixas.map(_.getLong(1))
//
//    val grafico = Pie(
//      labels = faixasEtarias,
//      values = quantidades
//    )
//
//    val layout = Layout(
//      title = "Distribuição de Pacientes por Faixa Etária"
//    )
//
//    Plotly.plot(
//      path = "distribuicao_faixas.html",
//      traces = Seq(grafico),
//      layout = layout
//    )
//
//  }
//}