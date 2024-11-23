//import org.apache.spark.sql.DataFrame
//import org.apache.spark.sql.functions._
//import plotly._
//import plotly.element._
//import plotly.layout._
//import plotly.Plotly
//
//object questao9 {
//  def run(dfRenomeado: DataFrame): Unit = {
//    dfRenomeado
//      .groupBy("diagnostico")
//      .agg(round(avg("Custo do tratamento"), 2).as("custoMedio"))
//      .orderBy(col("custoMedio").desc)
//    //  .show(false)
//
//    val custos = dfRenomeado
//      .groupBy("diagnostico")
//      .agg(round(avg("Custo do tratamento"), 2).as("custoMedio"))
//      .orderBy(col("custoMedio").desc)
//      .collect()
//
//    val diagnosticos = custos.map(_.getString(0))
//    val valores = custos.map(_.getDouble(1))
//
//    val grafico = Bar(
//      x = valores,
//      y = diagnosticos,
//      orientation = BarOrientation.Horizontal
//    )
//
//    val layout = Layout(
//      title = "Comparação de Custos Médios por Diagnóstico",
//      xaxis = Axis().withTitle("Custo Médio (R$)"),
//      yaxis = Axis().withTitle("Diagnóstico")
//    )
//
//    Plotly.plot(
//      path = "comparacao_custos.html",
//      traces = Seq(grafico),
//      layout = layout
//    )
//
//  }
//}