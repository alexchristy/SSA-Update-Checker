import logging
from s3_bucket import s3Bucket
import os

def gen_archive_dir_s3(s3: s3Bucket, terminalName: str) -> str:
    logging.info('Creating archive directories in s3 bucket: %s', s3.bucket_name)

    archiveDir = 'archive/'

    # Sub directories for sorting the different types of
    # PDFs a terminal can generate.
    dirTypes = ['72_HR/', '30_DAY/', 'ROLLCALL/']

    # Convert terminal name to snake case
    snakeCaseName = terminalName.replace(' ', '_')
    terminalArchiveDir = archiveDir + snakeCaseName + '/'

    try:
        # Create base terminal folder in archive if it doesn't exist.
        if not s3.directory_exists(terminalArchiveDir):
            s3.create_directory(terminalArchiveDir)
            logging.info('Created directory %s in s3.', terminalArchiveDir)

        for dirType in dirTypes:
            subDir = terminalArchiveDir + dirType

            if not s3.directory_exists(subDir):
                s3.create_directory(subDir)
                logging.info('Created sub directory %s in s3.', subDir)

    except Exception as e:
        logging.error(f"Error while generating archive directories for {terminalName} in bucket {s3.bucket_name}. Error: {str(e)}")
        raise

    return terminalArchiveDir


def check_s3_pdf_dirs(s3: s3Bucket):

    currentDir = 'current/'
    archiveDir = 'archive/'
    typeOfPdfDirs = ['72_HR/', '30_DAY/', 'ROLLCALL/']

    if not s3.directory_exists(currentDir):
        s3.create_directory(currentDir)

    for dirType in typeOfPdfDirs:
        currPath = os.path.join(currentDir, dirType)

        if not s3.directory_exists(currPath):
            s3.create_directory(currPath)
    
    if not s3.directory_exists(archiveDir):
        s3.create_directory(archiveDir)

# def archive_pdfs_s3(db: MongoDB, s3: s3Bucket, updatedTerminals: List[Terminal]):

#     for terminal in updatedTerminals:

#         # Retrieve terminal doc from DB
#         storedTerminal = db.get_doc_by_field_value('name', terminal.name)

#         # Get archive directory
#         if storedTerminal['archiveDir'] == 'empty':
#             archiveDir = gen_archive_dir_s3(s3, terminal.name)
#             db.set_terminal_field(terminal.name, 'archiveDir', archiveDir)
#         else:
#             archiveDir = storedTerminal['archiveDir']

#         # Check archive directory exists
#         if not s3.directory_exists(archiveDir):
#             logging.error(f'{archiveDir} was not found in S3. Skipping archive function...')
#             continue

#         # 72 hour schedule was updated
#         if terminal.is72HourUpdated:

#             # Retrieve the old current pdf
#             oldPdfPath = storedTerminal['pdfName72Hour']

#             # If there is a 72 hour pdf in s3
#             if oldPdfPath != 'empty':

#                 # Generate archive name for the PDF
#                 archiveName = gen_pdf_archive_name(terminal.name, '72_HR')

#                 # Generate archive path
#                 dest = os.path.join(archiveDir, '72_HR/')
#                 dest = os.path.join(dest, archiveName)

#                 # Move it to archive directory in s3
#                 s3.move_object(oldPdfPath, dest)
#                 logging.info(f'Archived {terminal.name} 72 hour pdf {oldPdfPath} at {dest}.')
#             else:
#                 logging.info(f'No 72 hour schedule to archive for {terminal.name}.')
            
#         # 30 Day schedule was updated
#         if terminal.is30DayUpdated:

#             # Retrieve the old current pdf
#             oldPdfPath = storedTerminal['pdfName30Day']

#             # If there is a 30 day pdf in s3
#             if oldPdfPath != 'empty':

#                 # Generate archive name for the PDF
#                 archiveName = gen_pdf_archive_name(terminal.name, '30_DAY')

#                 # Generate archive path
#                 dest = os.path.join(archiveDir, '30_DAY/')
#                 dest = os.path.join(dest, archiveName)

#                 # Move it to archive directory in s3
#                 s3.move_object(oldPdfPath, dest)
#                 logging.info(f'Archived {terminal.name} 30 day pdf {oldPdfPath} at {dest}.')
#             else:
#                 logging.info(f'No 30 day schedule to archive for {terminal.name}.')

#         # Rollcall was updated
#         if terminal.isRollcallUpdated:

#             # Retrieve the old current pdf
#             oldPdfPath = storedTerminal['pdfNameRollcall']

#             # If there is a rollcall pdf in s3
#             if oldPdfPath != 'empty':
#                 # Generate archive name for the PDF
#                 archiveName = gen_pdf_archive_name(terminal.name, 'ROLLCALL')

#                 # Generate archive path
#                 dest = os.path.join(archiveDir, 'ROLLCALL/')
#                 dest = os.path.join(dest, archiveName)

#                 # Move it to archive directory in s3
#                 s3.move_object(oldPdfPath, dest)
#                 logging.info(f'Archived {terminal.name} rollcall pdf {oldPdfPath} at {dest}.')
#             else:
#                 logging.info(f'No rollcall pdf to archive for {terminal.name}.')


# def rotate_pdfs_to_current_s3(db: MongoDB, s3: s3Bucket, updatedTerminals: List[Terminal]):

#     baseDir = os.getenv('PDF_DIR')

#     for terminal in updatedTerminals:

#         # If 72 hour is updated
#         if terminal.is72HourUpdated:

#             # Check if pdf still exists
#             tmpPdfPath = os.path.join(baseDir, terminal.pdfName72Hour)
#             if os.path.exists(tmpPdfPath):
#                 tmpPdfName = os.path.basename(terminal.pdfName72Hour)

#                 # Create s3 destination path
#                 dest = os.path.join('current/72_HR/', tmpPdfName)

#                 # Upload to s3 current directory
#                 s3.upload_to_s3(tmpPdfPath, dest)

#                 # Update the db to reflect new PDF
#                 db.set_terminal_field(terminal.name, 'pdfName72Hour', dest)
#                 db.set_terminal_field(terminal.name, 'pdfHash72Hour', terminal.pdfHash72Hour)
#                 db.set_terminal_field(terminal.name, 'is72HourUpdated', True)
#             else:
#                 logging.error(f'Unable to upload {tmpPdfPath} to s3.')

#         # If 30 day is updated
#         if terminal.is30DayUpdated:

#             # Check if pdf still exists
#             tmpPdfPath = os.path.join(baseDir, terminal.pdfName30Day)
#             if os.path.exists(tmpPdfPath):
#                 tmpPdfName = os.path.basename(terminal.pdfName30Day)

#                 # Create s3 destination path
#                 dest = os.path.join('current/30_DAY/', tmpPdfName)

#                 # Uploaded to s3 current directory
#                 s3.upload_to_s3(tmpPdfPath, dest)

#                 # Update the db to reflect the new PDF
#                 db.set_terminal_field(terminal.name, 'pdfName30Day', dest)
#                 db.set_terminal_field(terminal.name, 'pdfHash30Day', terminal.pdfHash30Day)
#                 db.set_terminal_field(terminal.name, 'is30DayUpdated', True)
#             else:
#                 logging.error(f'Unable to upload {tmpPdfPath} to s3.')
        
#         # If rollcall is updated
#         if terminal.isRollcallUpdated:

#             # Check if pdf still exists
#             tmpPdfPath = os.path.join(baseDir, terminal.pdfNameRollcall)
#             if os.path.exists(tmpPdfPath):
#                 tmpPdfName = os.path.basename(terminal.pdfNameRollcall)

#                 # Create s3 destination path
#                 dest = os.path.join('current/ROLLCALL/', tmpPdfName)

#                 # Uploaded to s3 current directory
#                 s3.upload_to_s3(tmpPdfPath, dest)

#                 # Update the db to reflect the new PDF
#                 db.set_terminal_field(terminal.name, 'pdfNameRollcall', dest)
#                 db.set_terminal_field(terminal.name, 'pdfHashRollcall', terminal.pdfHashRollcall)
#                 db.set_terminal_field(terminal.name, 'isRollcallUpdated', True)
#             else:
#                 logging.error(f'Unable to upload {tmpPdfPath} to s3.')   
