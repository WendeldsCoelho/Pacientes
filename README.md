# Análise de Dados Médicos com Scala, Spark e Plotly - Grupo 2

Este repositório representa o trabalho 1 da segunda unidade da matéria de Paradigmas de Linguagens de Programação no semestre 2024.2

## 1. Introdução
A combinação de Scala, Apache Spark e Plotly oferece uma solução poderosa para análise de dados moderna. O Scala, com seu paradigma funcional, permite escrever código conciso e eficiente, ideal para manipulação de grandes volumes de dados. O Spark, framework de computação distribuída, integra-se naturalmente ao Scala, proporcionando processamento paralelo escalável e otimizado.

Complementando, o Plotly permite criar visualizações interativas e intuitivas, facilitando a comunicação dos resultados para públicos diversos. Juntas, essas tecnologias oferecem uma abordagem completa, unindo processamento eficiente e visualização clara, tornando-as ideais para projetos de análise de dados.

## 2. Configuração do ambiente
### Versões necessárias
- **Scala(2.13.14)**: Linguagem de programação utilizada para desenvolver pipelines de dados e análises.
- **Apache Spark(3.5.3)**: Framework de processamento distribuído que permite manipular grandes volumes de dados de forma eficiente.
- **Plotly(0.8.4)**: Biblioteca para criação de gráficos e visualizações interativas.
- **JDK (11)**: Versão específica da JDK que é compatível com o scala.
- **IDE (foi utilizada o IntelliJ)**: Ambiente de Desenvolvimento compatível com o uso do scala.

### Arquivo Build.sbt:

```scala
    name := "Pacientes"

    version := "0.1"

    scalaVersion := "2.13.14"

    libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.3"
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.3"
    libraryDependencies += "org.plotly-scala" %% "plotly-render" % "0.8.4"

```

### Passo a Passo de instalação(Utilizando o IntelliJ)

1. Instalação do Scala e sbt Executor: 
   Efetue a instalação da Linguagem Scala na Versão 2.13.14 e a instalação do plugin do sbt Executor por meio dos plugins oficiais.
2. Configuração da JDK:
   Configure a JDK para a Versão 11, caso não possua essa Versão ela pode ser baixada diretamente pela IDE.
3. Clonagem do repositório:
   Clone este repositório para sua máquina com o comando:
   
```bash
    git clone https://github.com/WendeldsCoelho/Pacientes.git.
```

## 3. Manipulação dos dados:

Aqui estarão as 10 analises feitas a partir da base dados e das perguntas disponibilizadas pelo professor juntamente com nossa analise dos dados.
Observação: 
     Os nomes da colunas foram modificados no Main afim de facilitar a utilização em código
   
   ```scala
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
   ```
   
3.1 Questão 1: Pacientes de idade avançada com tratamento específico: Liste todos os atendimentos de pacientes com mais de 60 anos que receberam tratamento “Fisioterapia”. Exiba o ID do atendimento, o nome do paciente e o diagnóstico.
    A consulta nesta questão foi alterada para melhorar a exibição em forma de gráfico, sendo retirado o filtro de tratamento "Fisioterapia" e passando a mostrar todos os pacientes acima de 60 anos agrupados por tratamento.
   
```scala 
    // Consulta original: Filtrar pacientes com idade > 60 e tratamento de Fisioterapia
    //val dfFiltradoTerminal = dfRenomeado
    //  .filter(col("idade") > 60 &&col("tratamento") === "Fisioterapia")
    //  .select("atendimento", "nomePaciente", "diagnostico")
    //  .show(false)

    // Nova consulta: Filtrar pacientes com idade > 60
    val dfFiltrado = dfRenomeado
        .filter(col("idade") > 60)

    val resultadoColetado = dfFiltrado
        .groupBy("tratamento")
        .agg(count("tratamento").alias("quantidade_pacientes"))
        .collect()
        .map(row => (row.getAs[String]("tratamento"), row.getAs[Long]("quantidade_pacientes")))
```
Gráfico gerado: [HTML](https://github.com/WendeldsCoelho/Pacientes/blob/master/Graficos/Q1_grafico_pacientes_por_tratamento_idade_maior_60.html) | [Imagem](https://github.com/WendeldsCoelho/Pacientes/blob/master/Graficos/Q1_grafico_pacientes_por_tratamento_idade_maior_60.png)

3.2 Questão 2: Média de custo de tratamento: Calcule a média do custo de todos os tratamentos realizados. Exiba apenas o valor da média
    Nesta questão, foi adicionado o desvio médio para complementar na análise da média.   
    
```scala
    // Calcular a média do custo do tratamento
    val mediaCusto = dfRenomeado
      .agg(avg("custoTratamento").alias("mediaCusto"))
      .collect()
      .head
      .getDouble(0)
    
    // Adicionar a coluna de diferença absoluta em relação à média para calcular o desvio médio
    val dfComDesvios = dfRenomeado.withColumn(
      "desvioAbsoluto",
      abs(col("custoTratamento") - lit(mediaCusto))
    )
    // Cálculo do desvio médio
    val desvioMedio = dfComDesvios
      .agg(round(avg(col("desvioAbsoluto")), 2).as("desvioMedio"))
      .collect()
      .head
      .getDouble(0)
 ```
Gráfico gerado: [HTML](https://github.com/WendeldsCoelho/Pacientes/blob/master/Graficos/Q2_grafico_media_custo_com_desvio.html) | [Imagem](https://github.com/WendeldsCoelho/Pacientes/blob/master/Graficos/Q2_grafico_media_custo_com_desvio.png)

3.3 Questão 3: Tratamentos longos: Selecione todos os tratamentos com duração superior a 30 dias. Exiba o ID do atendimento, o tratamento, a duração e o médico responsável.
    A consulta nesta questão foi alterada para melhorar a exibição em forma de gráfico, agrupando os pacientes por tratamento ao invés de mostrar a quantidade total em conjunto.
    
```scala
    // Consulta original: Contar a quantidade de pacientes com duração > 30 dias
    //    val resultadoTerminal = dfRenomeado
    //      .filter(col("duracao") > 30)
    //      .select("atendimento", "tratamento", "duracao", "medico")
    //      .show(Int.MaxValue, truncate = false)
    //    Nova consulta: Quantidade de Pacientes por Tratamento (duração > 30 dias)
    val resultadoColetado = dfRenomeado
      .filter(col("duracao") > 30)
      .groupBy("tratamento")
      .agg(count("tratamento").alias("quantidade_pacientes"))
      .collect()
      .map(row => (row.getAs[String]("tratamento"), row.getAs[Long]("quantidade_pacientes")))
```
Gráfico gerado: [HTML](https://github.com/WendeldsCoelho/Pacientes/blob/master/Graficos/Q3_grafico_pacientes_por_tratamento.html) | [Imagem](https://github.com/WendeldsCoelho/Pacientes/blob/master/Graficos/Q3_grafico_pacientes_por_tratamento.png)

3.4 Questão 4: Média de idade dos pacientes: Calcule a média de idade dos pacientes atendidos. Exiba apenas o valor da média.
    Nesta questão, foi adicionado o desvio médio para complementar na análise da média.
    
```scala
      // Calcula a média da idade
        val mediaIdade = dfRenomeado
      .agg(avg("idade").alias("mediaIdade"))
      .collect()
      .head
      .getDouble(0)

        // Adiciona coluna de desvio absoluto para calcular o desvio médio
        val dfComDesvios = dfRenomeado.withColumn(
      "desvioAbsoluto",
      abs(col("idade") - lit(mediaIdade))
        )

        // Cálculo do desvio médio
        val desvioMedio = dfComDesvios
      .agg(round(avg("desvioAbsoluto"), 2).as("desvioMedio"))
      .collect()
      .head
      .getDouble(0)
```
Gráfico gerado: [HTML](https://github.com/WendeldsCoelho/Pacientes/blob/master/Graficos/Q4_grafico_media_idade_com_desvio.html)| [Imagem](https://github.com/WendeldsCoelho/Pacientes/blob/master/Graficos/Q4_grafico_media_idade_com_desvio.png)

3.5 Questão 5: Pacientes em tratamento ativo: Filtre todos os atendimentos cujo tratamento ainda esteja em andamento (A data atual é menor que a data de início acrescida da duração do tratamento). Exiba o ID do atendimento, o nome do paciente, o diagnóstico e o médico responsável.
    Nesta questão, nós utilizamos o método LocalDate.now() para capturar a data atual e usá-la de referência para a exibição dos tratamentos em andamento.
    
```scala
    // Obter a data atual como String no formato "yyyy-MM-dd"
    val dataReferencia = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))

    // Adiciona coluna calculada com a data final estimada do tratamento
    val dfComDataFim = dfRenomeado.withColumn(
      "data_fim_estimada",
      expr("date_add(dataInicio, duracao)")
    )

    // Filtragem dos tratamentos em andamento com base na data de referência
    val tratamentosAtivos = dfComDataFim
      .filter(col("data_fim_estimada") > to_date(lit(dataReferencia), "yyyy-MM-dd"))

    // Agrupa por médico e conta a quantidade de casos ativos
    val casosPorMedico = tratamentosAtivos
      .groupBy("medico")
      .agg(count("atendimento").alias("quantidade_casos"))
      .orderBy(desc("quantidade_casos"))
   ```
Gráfico gerado: [HTML](https://github.com/WendeldsCoelho/Pacientes/blob/master/Graficos/Q5_casos_ativos_por_medico.html) | [Imagem](https://github.com/WendeldsCoelho/Pacientes/blob/master/Graficos/Q5_casos_ativos_por_medico.png)

3.6 Questão 6: Atendimentos de um diagnóstico específico: Selecione todos os atendimentos com diagnóstico de “Hipertensão”. Exiba o ID do atendimento, o paciente, o tratamento e o custo
    A consulta nesta questão foi alterada para melhorar a exibição em forma de gráfico, sendo retirado o filtro de diagnóstico e agrupando todos os pacientes por diagnóstico.    
    
```scala
    // Consulta original: Contar a quantidade de pacientes com diagnóstico de "Hipertensão"
    //    val resultadoTerminal = dfRenomeado
    //      .filter(col("diagnostico") === "Hipertensão")
    //      .select("atendimento", "nomePaciente", "tratamento", "custoTratamento")
    //      .show(Int.MaxValue, truncate = false)
    //    Nova consulta: Quantidade de Pacientes por Diagnóstico
    val resultadoColetado = dfRenomeado
      .groupBy("diagnostico")
      .agg(count("diagnostico").alias("quantidade_pacientes"))
      .collect()
      .map(row => (row.getAs[String]("diagnostico"), row.getAs[Long]("quantidade_pacientes")))
```
Gráfico gerado: [HTML](https://github.com/WendeldsCoelho/Pacientes/blob/master/Graficos/Q6_grafico_pacientes_por_diagnostico.html) | [Imagem](https://github.com/WendeldsCoelho/Pacientes/blob/master/Graficos/Q6_grafico_pacientes_por_diagnostico.png)

3.7 Questão 7:Custo total de tratamentos de um médico específico: Calcule o custo total dos tratamentos administrados pelo médico “Dr. Silva”. Exiba apenas o valor do custo total.
    
A consulta nesta questão foi alterada para melhorar a exibição em forma de gráfico, sendo retirado o filtro de médico "Dr. Silva" e mostrando o custo total por médico.
    
```scala
    // Consulta original: calcular o custo total do "Dr. Silva"
    //    dfRenomeado
    //      .filter(col("medico") === "Dr. Silva")
    //      .agg(round(sum("CustoTratamento"), 2).as("custoTotal"))
    //      .select("custoTotal")
    //      .show(false)

    // Nova consulta: comparar custo total entre todos os médicos
    val resultadoColetado = dfRenomeado
      .groupBy("medico")
      .agg(round(sum("CustoTratamento"), 2).as("custoTotal"))
      .collect()
      .map(row => (row.getAs[String]("medico"), row.getAs[Double]("custoTotal")))
```
Gráfico gerado: [HTML](https://github.com/WendeldsCoelho/Pacientes/blob/master/Graficos/Q7_grafico_custo_total_por_medico.html) | [Imagem](https://github.com/WendeldsCoelho/Pacientes/blob/master/Graficos/Q7_grafico_custo_total_por_medico.png)

3.8 Questão 8:Duração média de todos os tratamentos: Calcule a duração média de todos os tratamentos em dias. Exiba apenas o valor da média.
    
Nesta questão, foi adicionado o desvio médio para complementar na análise da média.
    
```scala
    val mediaDuracao = dfRenomeado
      .agg(avg(col("duracao")).as("mediaDuracao"))
      .collect()
      .head
      .getDouble(0)

    // Adicionar a coluna da diferença absoluta em relação à média para calcular o desvio médio
    val dfComDesvios = dfRenomeado.withColumn(
      "desvioAbsoluto",
      abs(col("duracao") - lit(mediaDuracao))
    )

    // Cálculo do desvio médio
    val desvioMedio = dfComDesvios
      .agg(round(avg(col("desvioAbsoluto")), 2).as("desvioMedio"))
      .collect()
      .head
      .getDouble(0)
```
Gráfico gerado: [HTML](https://github.com/WendeldsCoelho/Pacientes/blob/master/Graficos/Q8_grafico_media_duracao_com_desvio.html) | [Imagem](https://github.com/WendeldsCoelho/Pacientes/blob/master/Graficos/Q8_grafico_media_duracao_com_desvio.png)

3.9 Questão 9:Comparação de custos entre diferentes diagnósticos: Quais diagnósticos geram os tratamentos mais caros ou baratos?
    Nesta questão, foi adicionado o desvio padrão para complementar na análise da média.

```scala
        // Custo médio por diágostico com desvio padrão
        val custosPorDiagnostico = dfRenomeado
          .groupBy("diagnostico")
          .agg(
            avg(col("custoTratamento")).alias("custo_medio"),
            stddev(col("custoTratamento")).alias("desvio_padrao")
          )
          .orderBy(col("custo_medio").desc)
```

Gráfico gerado: [HTML](https://github.com/WendeldsCoelho/Pacientes/blob/master/Graficos/Q9_grafico_custo_medio_por_diagnostico.html) | [Imagem](https://github.com/WendeldsCoelho/Pacientes/blob/master/Graficos/Q9_grafico_custo_medio_por_diagnostico.png)

3.10 Questão 10: Distribuição de pacientes por faixa etária: Qual faixa etária (0-18, 19-30, 31-60, 60+) é mais frequente nas consultas?
    Nesta questão, foram adicionadas as divisões por faixa etária para a exibição em gráfico.
    
```scala
        // Adição da coluna de faixa etária
        val dfFaixas = dfRenomeado
          .withColumn(
            "faixa_etaria",
            when(col("idade") <= 18, "0-18")
              .when(col("idade").between(19, 30), "19-30")
              .when(col("idade").between(31, 60), "31-60")
              .otherwise("60+")
          )
    
        // Contagem de pacientes por faixa etária
        val faixas = dfFaixas
          .groupBy("faixa_etaria")
          .count()
          .orderBy("faixa_etaria")
          .collect()
```

Gráfico gerado: [HTML](https://github.com/WendeldsCoelho/Pacientes/blob/master/Graficos/Q10_distribuicao_faixas_bar.html) | [Imagem](https://github.com/WendeldsCoelho/Pacientes/blob/master/Graficos/Q10_distribuicao_faixas_bar.png)
