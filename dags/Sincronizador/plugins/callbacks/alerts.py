from airflow.models import Variable
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

smtp_server = Variable.get('smtp_server', default_var='')
smtp_port = Variable.get('smtp_port', default_var='')
from_email = Variable.get('smtp_from_email', default_var='')
to_email = Variable.get('smtp_email_falha', default_var='')

def alerta_de_falha(context):
    # Envia mensagem Rocket
    #api = RocketChatAPI(settings={'username': bot_name, 'password': bot_password, 'domain': rocket_url})
    #api.send_message(f"A tarefa {context['task_instance_key_str']} falhou.", '#BI-Infraestrutura')
    # Envia e-mail
    partes = context['task_instance_key_str'].split('__')
    dag = partes[0]
    tarefa = partes[1]
    subject = f'Falha de execução na DAG {dag}'
    corpo_email = f"""
        <html>
            <body>
                <p>Olá,</p>
                <p>A tarefa <b>{tarefa}</b> da DAG <b>{dag}</b> falhou com a seguinte exceção:</p>
                <p><span style="color:red;">{context['exception']}</span>.</p>
                <p>Essa é uma mensagem automática, favor não responder.</p>
                <p>Atenciosamente,</p>
                <p>Equipe GEDAD</p>
            </body>
        </html>
        """
    msg = MIMEMultipart()
    msg['From'] = from_email
    msg['To'] = to_email
    msg['Subject'] = subject
    #msg.attach(MIMEText(f"A tarefa {context['task_instance_key_str']} falhou com a seguinte exceção:\n\n {context['exception']}.", 'plain'))
    msg.attach(MIMEText(corpo_email, 'html', 'utf-8'))
    server = smtplib.SMTP(smtp_server, smtp_port)
    server.starttls()  # Inicia a conexão TLS, se necessário
    server.sendmail(from_email, to_email, msg.as_string())
    server.quit()