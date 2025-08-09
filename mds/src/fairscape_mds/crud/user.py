from fairscape_mds.crud.fairscape_request import FairscapeRequest
from fairscape_mds.crud.fairscape_response import FairscapeResponse
from fairscape_mds.models.user import UserWriteModel

import datetime
import jwt


class FairscapeUserRequest(FairscapeRequest):
	
	def createUser(self, userInstance):
		userWriteInstance = UserWriteModel.model_validate({**userInstance.model_dump()})
		
		insertResult = self.config.userCollection.insert_one(
				userWriteInstance.model_dump(by_alias=True)
		)

		# check that insertResult was successfull

		return insertResult

	def loginUser(self, userEmail: str, userPassword: str):
		""" Get a user record, create a session for the 
		"""
	
		foundUser = self.config.userCollection.find_one({
				"email": userEmail,
				"password": userPassword
		})

		if foundUser is None:
				return FairscapeResponse(
					success=False,
					statusCode=401,
					jsonResponse={"error": "credentials not found"}
				)

		# create a token for the user
		userEmail = foundUser['email']
		fullname = ' '.join([foundUser['firstName'], foundUser['lastName']])
		now = datetime.datetime.now(datetime.timezone.utc)
		exp = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=24)

		nowTimestamp = datetime.datetime.timestamp(now)
		expTimestamp = datetime.datetime.timestamp(exp)

		tokenMessage = {
			'iss': 'https://fairscape.net/',
			'sub':  userEmail,
			'name': fullname,
			#'email': userEmail,
			'iat': int(nowTimestamp),
			'exp': int(expTimestamp)
		}

		compactJWS = jwt.encode(
				tokenMessage, 
				self.config.jwtSecret, 
				algorithm="HS256"
		)
		
		# set session in userCollection
		updateTokenResult = self.config.userCollection.update_one({
				"email": userEmail,
				"password": userPassword
				},
				{
				"$set": {"session": compactJWS}
				}
		)

		# check that update is correct
		if updateTokenResult.matched_count == 1 and updateTokenResult.modified_count == 1:

			return FairscapeResponse(
				success = True,
				jsonResponse = {"access_token": compactJWS},
				statusCode = 200
			)

		else:
			
			return FairscapeResponse(
				success = False,
				jsonResponse = {
					"error": "failed to set token"
					},
				statusCode = 500
			)


	def getUserBySession(self, session: str):

		tokenMetadata = jwt.decode(
			jwt=session,
			key=self.config.jwtSecret,
			algorithms=["HS256"]
		)

		userEmail = tokenMetadata.get('sub')

		foundUser = self.config.userCollection.find_one({
			"email": userEmail
		})

		if foundUser:
				return UserWriteModel.model_validate(foundUser)
		else:
				return None