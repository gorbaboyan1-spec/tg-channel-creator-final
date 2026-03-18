# Инструкция по деплою на Railway

## Шаг 1: Подготовка к деплою

✅ **Уже сделано:**
- Код обновлен для работы с переменными окружения
- Создан `.env.example` с примером переменных
- Создан `requirements.txt` с зависимостями
- Создан `Procfile` для запуска приложения

## Шаг 2: Инициализировать Git репозиторий

```bash
cd "/Users/user/Desktop/самый главный!! выгрузка"
git init
git add .
git commit -m "Initial commit"
```

## Шаг 3: Деплой на Railway

1. Перейдите на https://railway.app
2. Нажмите **"Sign Up"** и зарегистрируйтесь (можно через GitHub)
3. Нажмите **"New Project"**
4. Выберите **"Deploy from GitHub"**
5. Подключите свой GitHub репозиторий
6. Railway автоматически обнаружит `Procfile` и `requirements.txt`

## Шаг 4: Установить переменные окружения

В Dashboard Railroad после создания проекта:

1. Перейдите во вкладку **"Variables"**
2. Добавьте переменные из `.env.example`:
   - `API_ID` = `39759728`
   - `API_HASH` = `43cb9dc9f090c7f147d76eca7e6ebbbc`
   - `BOT_TOKEN` = `8192380712:AAFBEp99YqLpRKSvB_XIhyPH7ZXlprt9qTA`
   - `ADMIN_MASTER` = `2455228q`
   - `FLASK_ENV` = `production`

3. Сохраните

## Шаг 5: Развертывание

Railway автоматически начнет деплой. Вы получите публичный URL типа:
```
https://[your-app-name].railway.app
```

## Редактирование в будущем

Чтобы обновить приложение:

```bash
# 1. Внесите изменения в код
# 2. Закомитьте изменения
git add .
git commit -m "Описание изменений"

# 3. Отправьте на GitHub (Railway автоматически пересоберет приложение)
git push origin main
```

## Альтернативные хостинги (если Railway не подходит)

- **Render.com** - https://render.com (аналогично Railway)
- **Heroku** - https://heroku.com (платный, но был бесплатный)
- **PythonAnywhere** - https://pythonanywhere.com (специально для Python)

---

**Важно:** Никогда не делайте коммит файла `.env` - используйте только `.env.example`!
