import json
import aiohttp
import asyncio
import signal
import sys
import os
from aiogram import Bot, Dispatcher, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.utils import executor
from bs4 import BeautifulSoup

# Отладочное логирование
def debug_log(context: str, data: dict = None):
    print(f"\n⚡️ [DEBUG] {context}")
    if data:
        print(f"Данные состояния: {data}")

API_TOKEN = '7876727440:AAEhQz8z73OfqRj5numlxrVh0tjMEgoXAI0'
GROUP_CHAT_ID = '-1002321901390'
USER_CHAT_ID = '908619661'

bot = Bot(token=API_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)

# Создаем состояния для FSM
class Form(StatesGroup):
    select_manager = State()
    select_employee = State()
    select_action = State()
    edit_name = State()
    edit_master_id = State()
    new_employee_name = State()
    new_employee_master_id = State()

# Вспомогательные функции для работы с JSON
async def load_data():
    try:
        if os.path.exists('employees_data.json'):
            with open('employees_data.json', 'r', encoding='utf-8') as f:
                return json.load(f)
        return {"managers": []}
    except Exception as e:
        debug_log("Ошибка загрузки данных", {"error": str(e)})
        return {"managers": []}

async def save_data(data):
    try:
        with open('employees_data.json', 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        return True
    except Exception as e:
        debug_log("Ошибка сохранения данных", {"error": str(e)})
        return False

# Обработчики команд с кнопками "Назад"
@dp.message_handler(commands=['start'])
async def cmd_start(message: types.Message):
    debug_log("Начало работы", {"user": message.from_user.username})
    await Form.select_manager.set()
    data = await load_data()
    
    markup = types.InlineKeyboardMarkup()
    for manager in data['managers']:
        safe_login = manager['telegram_login'].replace('_', '%%UNDERSCORE%%')
        markup.add(types.InlineKeyboardButton(
            text=manager['telegram_login'],
            callback_data=f"manager_{safe_login}"
        ))
    markup.add(types.InlineKeyboardButton("❌ Отмена", callback_data="cancel"))
    
    await message.answer("Выбери РГ для редактирования:", reply_markup=markup)

@dp.callback_query_handler(lambda c: c.data.startswith('manager_'), state=Form.select_manager)
async def process_manager(callback_query: types.CallbackQuery, state: FSMContext):
    try:
        safe_login = callback_query.data.split('manager_')[1]
        manager_login = safe_login.replace('%%UNDERSCORE%%', '_')
        debug_log("Выбор менеджера", {"manager": manager_login})
        
        await state.update_data(selected_manager=manager_login)
        data = await load_data()
        manager = next((m for m in data['managers'] if m['telegram_login'] == manager_login), None)
        
        if not manager:
            await callback_query.answer("Менеджер не найден!")
            return

        markup = types.InlineKeyboardMarkup()
        for employee in manager['employees']:
            safe_name = employee['name'].replace('_', '%%UNDERSCORE%%')
            markup.add(types.InlineKeyboardButton(
                text=employee['name'],
                callback_data=f"employee_{safe_name}"
            ))
        markup.row(
            types.InlineKeyboardButton("Добавить сотрудника ➕", callback_data="add_employee"),
            types.InlineKeyboardButton("🔙 Назад", callback_data="back_to_start")
        )
        
        await Form.select_employee.set()
        await bot.delete_message(callback_query.from_user.id, callback_query.message.message_id)
        await bot.send_message(
            callback_query.from_user.id,
            "Выбери существующего сотрудника или добавь нового:",
            reply_markup=markup
        )
    except Exception as e:
        debug_log("Ошибка в process_manager", {"error": str(e)})
    await callback_query.answer()

@dp.callback_query_handler(lambda c: c.data == 'back_to_start', state=Form.select_employee)
async def back_to_managers(callback_query: types.CallbackQuery, state: FSMContext):
    await Form.select_manager.set()
    data = await load_data()
    
    markup = types.InlineKeyboardMarkup()
    for manager in data['managers']:
        safe_login = manager['telegram_login'].replace('_', '%%UNDERSCORE%%')
        markup.add(types.InlineKeyboardButton(
            text=manager['telegram_login'],
            callback_data=f"manager_{safe_login}"
        ))
    
    await bot.delete_message(callback_query.from_user.id, callback_query.message.message_id)
    await bot.send_message(
        callback_query.from_user.id,
        "Выбери РГ для редактирования:",
        reply_markup=markup
    )
    await callback_query.answer()

@dp.callback_query_handler(lambda c: c.data.startswith('employee_'), state=Form.select_employee)
async def process_employee(callback_query: types.CallbackQuery, state: FSMContext):
    try:
        safe_name = callback_query.data.split('employee_')[1]
        employee_name = safe_name.replace('%%UNDERSCORE%%', '_')
        debug_log("Выбор сотрудника", {"employee": employee_name})
        
        await state.update_data(selected_employee=employee_name)
        
        markup = types.InlineKeyboardMarkup()
        markup.row(
            types.InlineKeyboardButton("Редактировать ✏️", callback_data="action_edit"),
            types.InlineKeyboardButton("Удалить ❌", callback_data="action_delete")
        )
        markup.add(types.InlineKeyboardButton("🔙 Назад", callback_data="back_to_employees"))
        
        await Form.select_action.set()
        await bot.delete_message(callback_query.from_user.id, callback_query.message.message_id)
        await bot.send_message(
            callback_query.from_user.id,
            f"Выбран сотрудник: {employee_name}",
            reply_markup=markup
        )
    except Exception as e:
        debug_log("Ошибка в process_employee", {"error": str(e)})
    await callback_query.answer()

@dp.callback_query_handler(lambda c: c.data == 'back_to_employees', state=Form.select_action)
async def back_to_employees(callback_query: types.CallbackQuery, state: FSMContext):
    try:
        user_data = await state.get_data()
        manager_login = user_data.get('selected_manager')
        data = await load_data()
        manager = next((m for m in data['managers'] if m['telegram_login'] == manager_login), None)

        if not manager:
            await callback_query.answer("Менеджер не найден!")
            return

        markup = types.InlineKeyboardMarkup()
        for employee in manager['employees']:
            safe_name = employee['name'].replace('_', '%%UNDERSCORE%%')
            markup.add(types.InlineKeyboardButton(
                text=employee['name'],
                callback_data=f"employee_{safe_name}"
            ))
        markup.row(
            types.InlineKeyboardButton("Добавить сотрудника ➕", callback_data="add_employee"),
            types.InlineKeyboardButton("🔙 Назад", callback_data="back_to_start")
        )
        
        await Form.select_employee.set()
        await bot.delete_message(callback_query.from_user.id, callback_query.message.message_id)
        await bot.send_message(
            callback_query.from_user.id,
            "Выбери существующего сотрудника или добавь нового:",
            reply_markup=markup
        )
    except Exception as e:
        debug_log("Ошибка в back_to_employees", {"error": str(e)})
    await callback_query.answer()

@dp.callback_query_handler(lambda c: c.data == 'add_employee', state=Form.select_employee)
async def add_employee(callback_query: types.CallbackQuery, state: FSMContext):
    try:
        debug_log("Добавление нового сотрудника")
        await Form.new_employee_name.set()
        await bot.delete_message(callback_query.from_user.id, callback_query.message.message_id)
        await bot.send_message(
            callback_query.from_user.id,
            "Введите имя нового сотрудника:"
        )
    except Exception as e:
        debug_log("Ошибка в add_employee", {"error": str(e)})
    await callback_query.answer()

@dp.message_handler(state=Form.new_employee_name)
async def process_new_employee_name(message: types.Message, state: FSMContext):
    try:
        debug_log("Ввод имени нового сотрудника", {"name": message.text})
        await state.update_data(new_employee_name=message.text)
        await Form.new_employee_master_id.set()
        await message.answer("Введите masterID нового сотрудника:")
    except Exception as e:
        debug_log("Ошибка в process_new_employee_name", {"error": str(e)})

@dp.message_handler(state=Form.new_employee_master_id)
async def process_new_employee_master_id(message: types.Message, state: FSMContext):
    try:
        master_id = int(message.text)
        debug_log("Ввод masterID нового сотрудника", {"master_id": master_id})
        user_data = await state.get_data()
        data = await load_data()
        
        manager = next((m for m in data['managers'] if m['telegram_login'] == user_data['selected_manager']), None)
        if manager:
            manager['employees'].append({
                "masterID": master_id,
                "name": user_data['new_employee_name']
            })
            if await save_data(data):
                await message.answer("✅ Сотрудник успешно добавлен!")
            else:
                await message.answer("❌ Ошибка при сохранении данных")
        await state.finish()
    except ValueError:
        await message.answer("❌ MasterID должен быть числом! Попробуйте снова:")
    except Exception as e:
        debug_log("Ошибка в process_new_employee_master_id", {"error": str(e)})
        await state.finish()

@dp.callback_query_handler(lambda c: c.data.startswith('action_'), state=Form.select_action)
async def process_action(callback_query: types.CallbackQuery, state: FSMContext):
    try:
        parts = callback_query.data.split('_', 1)
        if len(parts) != 2:
            await callback_query.answer("Некорректный запрос")
            return
        
        action = parts[1]
        debug_log("Выбор действия", {"action": action})
        
        user_data = await state.get_data()
        
        if action == 'edit':
            await Form.edit_name.set()
            await bot.delete_message(callback_query.from_user.id, callback_query.message.message_id)
            await bot.send_message(
                callback_query.from_user.id,
                "Введите новое имя сотрудника:"
            )
        elif action == 'delete':
            data = await load_data()
            manager = next((m for m in data['managers'] if m['telegram_login'] == user_data['selected_manager']), None)
            if manager:
                manager['employees'] = [e for e in manager['employees'] if e['name'] != user_data['selected_employee']]
                if await save_data(data):
                    await bot.send_message(callback_query.from_user.id, "✅ Сотрудник успешно удален!")
                else:
                    await bot.send_message(callback_query.from_user.id, "❌ Ошибка при сохранении данных")
            await state.finish()
    except Exception as e:
        debug_log("Ошибка в process_action", {"error": str(e)})
    await callback_query.answer()

@dp.message_handler(state=Form.edit_name)
async def process_edit_name(message: types.Message, state: FSMContext):
    try:
        new_name = message.text
        debug_log("Ввод нового имени", {"new_name": new_name})
        await state.update_data(new_name=new_name)
        await Form.edit_master_id.set()
        await message.answer("Введите новый masterID:")
    except Exception as e:
        debug_log("Ошибка в process_edit_name", {"error": str(e)})

@dp.message_handler(state=Form.edit_master_id)
async def process_edit_master_id(message: types.Message, state: FSMContext):
    try:
        new_master_id = int(message.text)
        debug_log("Ввод нового masterID", {"master_id": new_master_id})
        
        user_data = await state.get_data()
        data = await load_data()
        
        manager = next((m for m in data['managers'] if m['telegram_login'] == user_data['selected_manager']), None)
        if manager:
            employee = next((e for e in manager['employees'] if e['name'] == user_data['selected_employee']), None)
            if employee:
                employee['name'] = user_data['new_name']
                employee['masterID'] = new_master_id
                if await save_data(data):
                    await message.answer("✅ Данные сотрудника успешно обновлены!")
                else:
                    await message.answer("❌ Ошибка при сохранении данных")
        await state.finish()
    except ValueError:
        await message.answer("❌ MasterID должен быть числом! Попробуйте снова:")
    except Exception as e:
        debug_log("Ошибка в process_edit_master_id", {"error": str(e)})
        await state.finish()

@dp.callback_query_handler(lambda c: c.data == 'cancel', state='*')
async def cancel_handler(callback_query: types.CallbackQuery, state: FSMContext):
    try:
        await state.finish()
        await bot.delete_message(callback_query.from_user.id, callback_query.message.message_id)
        await bot.send_message(callback_query.from_user.id, "Операция отменена")
    except Exception as e:
        debug_log("Ошибка в cancel_handler", {"error": str(e)})
    await callback_query.answer()

# Оригинальный код парсинга и уведомлений
async def send_error_message(error_message):
    try:
        await bot.send_message(USER_CHAT_ID, f"Ошибка: {error_message}")
    except Exception as e:
        print(f"Не удалось отправить сообщение об ошибке в Telegram: {e}")

async def load_sent_links():
    if os.path.exists('sent_links.json'):
        with open('sent_links.json', 'r', encoding='utf-8') as f:
            return json.load(f)
    return {"sent_links": []}

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
    print("⚡️ Бот запущен. Ожидаем событий...")
    signal.signal(signal.SIGTERM, handle_exit)
    signal.signal(signal.SIGINT, handle_exit)
    executor.start_polling(dp, on_startup=on_startup, skip_updates=True)
