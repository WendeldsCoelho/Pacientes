import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import plotly._
import plotly.element._
import plotly.layout._
import plotly.Plotly

object questao1 {
  def run(dfRenomeado: DataFrame): Unit = {
    // Filtrar dados de pacientes acima de 60 anos que receberam "Fisioterapia"
    val resultado = dfRenomeado
      .filter(col("idade") > 60 && col("tratamento") === "Fisioterapia")
      .select("atendimento", "nomePaciente", "diagnostico")
      .collect()

    // Converter resultados para Seq (necessário para Plotly)
    val nomes: Seq[String] = resultado.map(_.getString(1)).toSeq  // Extrai "nomePaciente" como Seq[String]
    val atendimentos: Seq[String] = resultado.map(_.getInt(0).toString).toSeq // Extrai "atendimento" e converte para Seq[String]

    // Criar o gráfico de barras
    val grafico = Bar(
      x = nomes,       // Eixo X: Nomes dos pacientes
      y = atendimentos // Eixo Y: IDs dos atendimentos como String
    )

    // Configurar layout do gráfico usando encadeamento
    // Configurar layout do gráfico com ajustes visuais
    val layout = Layout()
      .withTitle("Atendimentos - Fisioterapia (> 60 Anos)")
      .withXaxis(
        Axis()
          .withTitle("Nome dos Pacientes")
          .withTickangle(-45) // Rota os rótulos do eixo X em -45 graus
          .withTickfont(Font().withSize(10)) // Reduz o tamanho da fonte dos rótulos
      )
      .withYaxis(Axis().withTitle("Atendimentos"))
      .withMargin(Margin(60, 30, 50, 100))  // Margens: esquerda, direita, superior, inferior

    // Gerar e salvar o gráfico
    val caminhoArquivo = "grafico_fisioterapia.html"
    Plotly.plot(
      path = caminhoArquivo,           // Caminho do arquivo de saída
      traces = Seq(grafico),           // Lista de gráficos (apenas 1 nesse caso)
      layout = layout,                 // Layout configurado
      config = Config(),               // Configuração padrão
      useCdn = true,                   // Usa CDN para carregar bibliotecas
      openInBrowser = true,            // Abre automaticamente no navegador
      addSuffixIfExists = true         // Adiciona sufixo se o arquivo já existir
    )

    println(s"Gráfico salvo e aberto no navegador: $caminhoArquivo")
  }
}