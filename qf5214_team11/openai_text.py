import csv
import time
from openai import OpenAI

client = OpenAI(api_key="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")
# save = []
csv_file_path = "./top50_traded_stock_news2.csv"
output_file_path = "./new_news.csv"

with open(csv_file_path) as csvfile:
    reader = csv.reader(csvfile)
    header = next(reader)
    header.append("score")

    with open(output_file_path, 'w', newline='') as outputfile:
        writer = csv.writer(outputfile)
        writer.writerow(header)

        for row in reader:
            content = row[6]  # column 7

            response = client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[
                    {
                        "role": "system",
                        "content": f"""
                    Task: Analyze the impact of the news on stock prices. \
                    Instructions: As a seasoned finance expert specializing in the stock market, you possess a keen understanding \
                    of how news articles can influence market dynamics. In this task, you will be provided with a news article \
                    or analysis. Upon thoroughly reading the article, please provide the sentimental \
                    anticipated impact of the news. The Sentiment value should range between 1.0 and 5.0, with 1.0 signifying \
                    highly negative news likely to cause significant decline in the stock price in the coming days/weeks, \
                    and 5.0 representing highly positive news likely to lead a surge in share price in the next few days/weeks. \
                    Please do not give an ambiguous answer and return the Sentiment value and keep it with one decimals. \
                    Your response must be strictly in the JSON format. Consider the following factors while determining the impact: \
                    The magnitude of the news, The sentiment of the news, Market conditions at the date of the news, Liquidity \
                    of the stock, The sector in which the company operates. The JSON response should include the number. \
                    Do not consider indices such as NIFTY. Do not invent values; maintain, even accuracy and integrity in response. \
                    All the news articles are related to at least one company. Please return as the below format in example. \
                    please as far as possible avoid giving 1.0 or 5.0 unless the news article is extremely inclined.
            
                    Example:
                  
                    Article: "Apple agreed to pay up to $14.4 million (CAD) to settle a class action lawsuit in Canada that " 
                               "alleged the company "
                               "secretly throttled the performance of some iPhone models batterygate, and eligibl… [+2033 chars]"
                               
                    Response: 2.3
                    
                    the response must only be a number with one decimal. Do not contain other infomation.
                    
                    """
                    },
                    {
                        "role": "user",
                        "content": content
                    }
                ],
                temperature=0.2,
                max_tokens=256,
                top_p=1,
                frequency_penalty=0,
                presence_penalty=0
            )

            print(response.choices[0].message.content)
            # save.append(response.choices[0].message.content)
            model_response = response.choices[0].message.content
            row.append(model_response)
            writer.writerow(row)
            time.sleep(0.5)

print("新的CSV文件已生成:", outputfile)