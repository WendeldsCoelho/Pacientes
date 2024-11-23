//import org.apache.spark.sql.DataFrame
//import org.apache.spark.sql.functions._
//import plotly._
//import plotly.element._
//import plotly.layout._
//import plotly.Plotly
//
//object questao7 {
//  def run(dfRenomeado: DataFrame): Unit = {
//    dfRenomeado
//      .filter(col("Médico responsável") === "Dr. Silva")  // Filtra apenas os tratamentos do Dr. Silva
//      .agg(round(sum("Custo do tratamento"), 2).as("custoTotal"))  // Calcula o custo total e arredonda para duas casas
//      .select("custoTotal")  // Seleciona apenas a coluna do custo total
//    //  .show(false)  // Exibe o valor sem truncamento
//
//    val custoTotal = dfRenomeado
//      .filter(col("Médico responsável") === "Dr. Silva")
//      .agg(round(sum("Custo do tratamento"), 2).as("custoTotal"))
//      .collect()(0).getDouble(0)
//
//    val layout = Layout(
//      title = "Custo Total - Dr. Silva"
//    )
//
//    val grafico = Indicator(
//      mode = IndicatorMode.Number,
//      value = custoTotal,
//      title = "Custo Total (R$)"
//    )
//
//    Plotly.plot(
//      path = "custo_total.html",
//      traces = Seq(grafico),
//      layout = layout
//    )
//
//  }
//}