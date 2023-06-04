from curses.ascii import isdigit
import logging
from mongodb import MongoDB
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters
from main import api_token

# Enable logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO

)
logger = logging.getLogger(__name__)

# Intialize MongoDB
db = MongoDB("SmartSpaceA", "Terminals")

# Function for generating list of subcriptions a user has
def generateSubscribedTerminalList(chatID):
    # Retrieve subscribed terminals
    subscribed_terminals = db.getSubscribedTerminals(chatID)

    # Sort the terminals by 'pagePosition'
    subscribed_terminals.sort(key=lambda terminal: terminal['pagePosition'])

    # Initialize the message
    message = "You are subscribed to:\n"

    # Generate list message
    for terminal in subscribed_terminals:
        message += f"({terminal['pagePosition']}) {terminal['name']}\n"

    return message

# Initialize menu map
documents = db.getAllTerminals()

positionToNameMap = {}
nameToPositionMap = {}

for doc in documents:
    name = doc['name']
    page_position = doc['pagePosition']

    positionToNameMap[page_position] = name
    nameToPositionMap[name] = page_position

# Generate /listall html menus
def initListallMessages():
    # Get all documents
    documents = db.getAllTerminals()

    # Sort the documents by 'group' and 'pagePosition'
    documents.sort(key=lambda doc: (doc['group'], doc['pagePosition']))

    # Initialize variables
    current_group = None
    message = ""
    messages = {}

    # Iterate over the sorted documents
    for doc in documents:
        # If we've moved to a new group, start a new message
        if doc['group'] != current_group:
            if current_group is not None:
                # Append the completed message for the previous group
                messages[current_group.split()[0].upper()] = message
            # Start a new message with the new group name in bold
            current_group = doc['group']
            message = f"<b>{current_group}</b>\n"

        # Add the current terminal to the message
        message += f"({doc['pagePosition']}) {doc['location']}\n"

    # Append the last message
    messages[current_group.split()[0].upper()] = message

    return messages

listallOptions = initListallMessages()

# Define a few command handlers. These usually take the two arguments update and
# context.
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send a message when the command /start is issued."""

    user = update.effective_user

    message = '''<b>Welcome to SmartSpaceA!</b>
This is a bot that will send you updates when flight information changes for a SpaceA terminal.

<b>Commands:</b>
/subscribe: Track updates for specific SpaceA terminals.
/listall: List the available SpaceA terminals.
/unsubscribe: Stop tracking updates for a terminal.
/get: Get most up to date schedule for a specific terminal.'''

    await update.message.reply_html(message)



async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send a message when the command /help is issued."""

    await update.message.reply_text("Help!")



async def subscribe_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Subscribe user to updates for terminal when the command /subscibe is issued."""

    # Messages
    error_message = '''To use to the /subscribe command you must specify <b>only</b> one number. Use /listall to to find the terminal number.
<b>Example:</b> /subscribe 1'''

    # Error handling
    # Invalid number of arguments
    if len(context.args) != 1:
        await update.message.reply_html(error_message)
        return

    # Arguement not a digit
    requestTerminalNum = context.args[0]
    if not requestTerminalNum.isdigit():
        await update.message.reply_html(error_message)
        return
    
    # Arguement out of bounds
    requestTerminalNum = int(requestTerminalNum)
    if requestTerminalNum not in positionToNameMap:
        await update.message.reply_html(error_message)
        return

    # Add subscription to Mongo
    terminalName = positionToNameMap[requestTerminalNum]
    terminal = db.getTerminalByName(terminalName)
    pdfPath = './pdfs/' + terminal.pdfName3Day
    chatID = update.effective_chat.id

    isSubscribed = db.isUserSubscribed(terminalName, chatID)

    if isSubscribed:
        await update.message.reply_text('You already subscribed to this terminal.')
    else:

        if terminal.pdfName3Day is None:
            await update.message.reply_text('Sorry! This schedule is not currently available.')
            return
        
        db.addSubscription(terminalName, chatID)
        await update.message.reply_text('You have been subscribed to ' + positionToNameMap[requestTerminalNum] + '.')
        await update.message.reply_text('Here is the most recent schedule:')

        # Open the PDF file in binary mode and send it
        with open(pdfPath, 'rb') as pdf_file:
            await update.message.reply_document(pdf_file)


async def unsubscribe_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Unsubscribe user to updates for specific terminals."""

    # Messages
    error_message = '''To use to the /unsubscribe command you must specify <b>only</b> one number. Use /listall to to find the terminal number.
<b>Example:</b> /unsubscribe 1'''

    # Error handling
    # Invalid number of arguments
    if len(context.args) != 1:
        await update.message.reply_html(error_message)
        return

    # Arguement not a digit
    requestTerminalNum = context.args[0]
    if not requestTerminalNum.isdigit():
        await update.message.reply_html(error_message)
        return
    
    # Arguement out of bounds
    requestTerminalNum = int(requestTerminalNum)
    if requestTerminalNum not in positionToNameMap:
        await update.message.reply_html(error_message)
        return

    # Remove subscription
    terminalName = positionToNameMap[requestTerminalNum]
    terminal = db.getTerminalByName(terminalName)
    chatID = update.effective_chat.id

    isSubscribed = db.isUserSubscribed(terminalName, chatID)

    if isSubscribed:
        db.removeSubscription(positionToNameMap[requestTerminalNum], update.effective_chat.id)
        await update.message.reply_text('You have been unsubscribed from ' + positionToNameMap[requestTerminalNum] + '.')

    else:
        await update.message.reply_text('You are not subscribed to this terminal.')

async def listall_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:

    """Send a message when the command /listall is issued."""

    errorMessage = '''To use the /listall command specify <b>one</b> of the valid regions after to list all terminals for that region.
    
<b>Valid Regions:</b>
AMC (Continental US)
EUCOM (Europe)
INDOPACOM (Asia)
CENTCOM (Middle East)
SOUTHCOM (South America)
NON-AMC    

<b>Example:</b>
/listall amc'''

    # Error handling
    validRegions = ['AMC', 'EUCOM', 'INDOPACOM', 'CENTCOM', 'SOUTHCOM', 'NON-AMC']

    # Check arguments
    # Wrong number of args
    if len(context.args) != 1:
        await update.message.reply_html(errorMessage)
        return

    requestedRegion = context.args[0]
    # Not a valid region
    if requestedRegion.upper() not in [region.upper() for region in validRegions]:
        await update.message.reply_html(errorMessage)
        return

    # Determine requested region and display terminals
    if requestedRegion.upper() == 'AMC':
        await update.message.reply_html(listallOptions['AMC'])

    elif requestedRegion.upper() == 'EUCOM':
        await update.message.reply_html(listallOptions['EUCOM'])

    elif requestedRegion.upper() == 'INDOPACOM':
        await update.message.reply_html(listallOptions['INDOPACOM'])
    
    elif requestedRegion.upper() == 'CENTCOM':
        await update.message.reply_html(listallOptions['CENTCOM'])

    elif requestedRegion.upper() == 'SOUTHCOM':
        await update.message.reply_html(listallOptions['SOUTHCOM'])
    
    elif requestedRegion.upper() == 'NON-AMC':
        await update.message.reply_html(listallOptions['NON-AMC'])

    else:
        await update.message.reply_text("Error selecting region!")
        return

async def get_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send a message when the command /get is issued."""
     
     # Messages
    error_message = '''To use to the /get command you must specify <b>only</b> one number. Use /listall to to find the terminal number.
    <b>Example:</b> /subscribe 1'''

    # Error handling
    # Invalid number of arguments
    if len(context.args) != 1:
        await update.message.reply_html(error_message)
        return

    # Arguement not a digit
    requestTerminalNum = context.args[0]
    if not requestTerminalNum.isdigit():
        await update.message.reply_html(error_message)
        return

    # Arguement out of bounds
    requestTerminalNum = int(requestTerminalNum)
    if requestTerminalNum not in positionToNameMap:
        await update.message.reply_html(error_message)
        return
    
    # Get PDF
    terminalName = positionToNameMap[requestTerminalNum]
    terminal = db.getTerminalByName(terminalName)

    if terminal.pdfName3Day is None:
        await update.message.reply_text('Sorry! This schedule is not currently available.')
        return

    pdfPath = './pdfs/' + terminal.pdfName3Day

    await update.message.reply_text('Here is the most recent schedule:')

    # Open the PDF file in binary mode and send it
    with open(pdfPath, 'rb') as pdf_file:
        await update.message.reply_document(pdf_file)

async def alerts_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    '''List all subscriptons /alerts'''

    # Messages
    error_message = '''To use this command type /alerts'''

    # Error handling
    # Invalid number of arguments
    if len(context.args) != 0:
        await update.message.reply_html(error_message)
        return
    
    subcribedTerminals = db.getSubscribedTerminals(update.effective_chat.id)
    chatID = update.effective_chat.id

    if len(subcribedTerminals) == 0:
        await update.message.reply_text('You are not subscribed to any terminals!')
        return
    
    elif len(subcribedTerminals) > 0:
        subListMessage = generateSubscribedTerminalList(chatID)
        await update.message.reply_html(subListMessage)
        return


async def echo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:

    """Echo the user message."""

    await update.message.reply_text(update.message.text)


def main() -> None:

    """Start the bot."""

    # Create the Application and pass it your bot's token.
    application = Application.builder().token(api_token).build()


    # on different commands - answer in Telegram
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("subscribe", subscribe_command))
    application.add_handler(CommandHandler("listall", listall_command))
    application.add_handler(CommandHandler("unsubscribe", unsubscribe_command))
    application.add_handler(CommandHandler("get", get_command))
    application.add_handler(CommandHandler("alerts", alerts_command))


    # on non command i.e message - echo the message on Telegram
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, echo))


    # Run the bot until the user presses Ctrl-C
    application.run_polling()

if __name__ == "__main__":
    main()