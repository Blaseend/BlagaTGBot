import json
import aiohttp
import asyncio
import signal
import sys
from aiogram import Bot, Dispatcher
from aiogram.utils import executor
from bs4 import BeautifulSoup
import os

API_TOKEN = '7876727440:AAEhQz8z73OfqRj5numlxrVh0tjMEgoXAI0'
GROUP_CHAT_ID = '-1002321901390'
USER_CHAT_ID = '908619661'  # Telegram ID

bot = Bot(token=API_TOKEN)
dp = Dispatcher(bot)

async def send_shutdown_message(reason):
    try:
        await bot.send_message(USER_CHAT_ID, f"Ой-ой, я неожиданно выключился. Причина: {reason}")
    except Exception as e:
        print(f"Не удалось отправить сообщение об остановке: {e}")

def handle_exit(signal_received, frame):
    asyncio.create_task(send_shutdown_message("Сигнал завершения"))
    sys.exit(0)

async def notify_about_thanks(master_id, name, link, manager_login, sent_links):
    if link not in sent_links['sent_links']:
        try:
            message = (f"Найдена благодарность у сотрудника с masterID: {master_id} ({name}). "
                       f"Ссылка на благодарность: {link}. "
                       f"Уведомляем: {manager_login}.")
            await bot.send_message(GROUP_CHAT_ID, message)
            sent_links['sent_links'].append(link)
            save_sent_links(sent_links)
        except Exception as e:
            await send_error_message(f"Не удалось отправить уведомление в групповой чат: {str(e)}")
    else:
        print(f"Ссылка {link} уже была отправлена ранее, пропускаем отправку.")

def save_sent_links(sent_links):
    try:
        with open('sent_links.json', 'w', encoding='utf-8') as f:
            json.dump(sent_links, f, ensure_ascii=False, indent=4)
    except Exception as e:
        print(f"Ошибка сохранения ссылок: {e}")

def load_sent_links():
    if os.path.exists('sent_links.json'):
        with open('sent_links.json', 'r', encoding='utf-8') as f:
            return json.load(f)
    return {"sent_links": []}

async def send_error_message(error_message):
    try:
        await bot.send_message(USER_CHAT_ID, f"Ошибка: {error_message}")
    except Exception as e:
        print(f"Не удалось отправить сообщение об ошибке в Telegram: {e}")

async def load_data():
    try:
        if os.path.exists('employees_data.json'):
            with open('employees_data.json', 'r', encoding='utf-8') as f:
                return json.load(f)
        else:
            raise FileNotFoundError('Файл employees_data.json не найден.')
    except FileNotFoundError as e:
        await send_error_message(str(e))
        return {"managers": []}
    except json.JSONDecodeError as e:
        await send_error_message(f"Ошибка чтения JSON: {str(e)}")
        return {"managers": []}

async def find_text_in_review(session, review_url, employees, semaphore, sent_links):
    async with semaphore:
        full_url = "https://www.banki.ru" + review_url
        try:
            async with session.get(full_url) as response:
                text = await response.text()
                soup = BeautifulSoup(text, 'html.parser')

                # <div> с классом "lf4cbd87d ld6d46e58 lb9ca4d21"
                ignored_element = soup.find('div', class_="lf4cbd87d ld6d46e58 lb9ca4d21")
                if ignored_element:
                    ignored_element.decompose()  # Удаляем элемент из DOM

                # <main> с классом "layout-wrapper"
                main_content = soup.find('main', class_="layout-wrapper")
                
                if main_content:
                    #  <main> в текст
                    main_text = main_content.get_text()

                    # Выполняем поиск 
                    for manager in employees:
                        for employee in manager['employees']:
                            master_id = employee['masterID']
                            name = employee['name']
                            if str(master_id) in main_text:
                                await notify_about_thanks(master_id, name, full_url, manager['telegram_login'], sent_links)
                                break
                else:
                    await send_error_message(f"Не удалось найти <main> на странице {full_url}")

        except Exception as e:
            await send_error_message(f"Ошибка парсинга {full_url}: {str(e)}")


async def parse_page(session, url, employees, semaphore, sent_links):
    try:
        async with session.get(url) as response:
            text = await response.text()
            soup = BeautifulSoup(text, 'html.parser')
            reviews = soup.find_all('a', href=True)
            review_links = list(set(a['href'] for a in reviews if '/services/responses/bank/response/' in a['href']))
            tasks = [find_text_in_review(session, review_url, employees, semaphore, sent_links) for review_url in review_links]
            await asyncio.gather(*tasks)
    except Exception as e:
        await send_error_message(f"Ошибка парсинга страницы {url}: {str(e)}")

async def parse_pages(employees, start_page=1, end_page=25):
    try:
        semaphore = asyncio.Semaphore(10)
        sent_links = load_sent_links()
        async with aiohttp.ClientSession() as session:
            tasks = [parse_page(session, f"https://www.banki.ru/services/responses/bank/tcs/?page={page}&type=all", employees, semaphore, sent_links) for page in range(start_page, end_page + 1)]
            await asyncio.gather(*tasks)
    except Exception as e:
        await send_error_message(f"Ошибка в процессе парсинга: {str(e)}")

async def schedule_parsing():
    employees = await load_data()
    while True:
        try:
            await bot.send_message(USER_CHAT_ID, "Начинаем цикл поиска")  # Сообщение вам о начале цикла
            await parse_pages(employees["managers"])
            await bot.send_message(USER_CHAT_ID, "Цикл поиска завершен")  # Сообщение вам о завершении цикла
        except Exception as e:
            await send_error_message(f"Ошибка при циклическом запуске парсинга: {str(e)}")
        await asyncio.sleep(3600)

async def send_startup_message():
    try:
        file_id = "CAACAgIAAxkBAAICemcb4CRF3xph4u6El4k_q2T_Er6zAAJCRAACMTNJS2iiZaxSRU60NgQ"
        await bot.send_sticker(GROUP_CHAT_ID, file_id)
    except Exception as e:
        await send_error_message(f"Ошибка при отправке стартового сообщения: {str(e)}")

async def on_startup(_):
    await send_startup_message()
    asyncio.create_task(schedule_parsing())

if __name__ == '__main__':
    signal.signal(signal.SIGTERM, handle_exit)
    signal.signal(signal.SIGINT, handle_exit)

    executor.start_polling(dp, on_startup=on_startup, skip_updates=True)
