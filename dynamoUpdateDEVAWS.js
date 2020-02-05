/*******************************************************************************
* Copyright 2019 Amazon.com, Inc. and its affiliates. All Rights Reserved.
*
* Licensed under the Amazon Software License (the "License").
* You may not use this file except in compliance with the License.
* A copy of the License is located at
*
*   http://aws.amazon.com/asl/
*
* or in the "license" file accompanying this file. This file is distributed
* on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
* express or implied. See the License for the specific language governing
* permissions and limitations under the License.
* @author Solution Builders
* @function updateDynamo
* @description tget the dynamodb table primary key (event.guid) and then loop through the
* event key:values and update Dynamo.
*
********************************************************************************/

const AWS = require('aws-sdk');
const error = require('./lib/error.js');

exports.handler = async (event) => {
	console.log('REQUEST:: ', JSON.stringify(event, null, 2));

	const dynamo = new AWS.DynamoDB.DocumentClient({
		region: process.env.AWS_REGION
	});

	try {
		
		//GET EXPERIENCE METADATA FROM S3 OBJECT AND ADD TO THE EVENT SO IT CAN BE SENT TO FIREBASE
		var s3 = new AWS.S3()
		var mdparams = {
			Bucket: "viewify-source-1hhupvqw8kb6h",
			Key: event.srcVideo
		};
		
		//ADD PHOTO/VIDEO CAPTION TO EVENT METADATA
		const metadataPromise = new Promise(function(resolve, reject) {

			s3.headObject(mdparams, function(err, data) {
				if (err) {
					//Error occurred 
					console.log(err, err.stack);
					event.caption = "Error"
					reject(err);
					
				}
				else {
					//Successful response
					console.log(data);
					
					/*
					event.caption = "Test"
					event.userid = "Test"
					event.storyid = "Test"
					resolve("ios metadata grabbed");
					*/
					
					
					
					event.caption = data["Metadata"]["caption"]
					event.userid = data["Metadata"]["userid"]
					event.storyid = data["Metadata"]["storyid"]
					event.draftcontentid  = data["Metadata"]["draftcontentid"]
					resolve("ios metadata grabbed");
					
					
					
				} 
		
			});

		})
		
		await metadataPromise;
		
		
		//remove guid from event data (primary db table key) and iterate over event objects
		// to build the update parameters
		let guid = event.guid;
		delete event.guid;
		let expression = '';
		let values = {};
		let i = 0;

		Object.keys(event).forEach((key) => {
			i++;
			expression += ' ' + key + ' = :' + i + ',';
			values[':' + i] = event[key];
		});

		let params = {
			TableName: process.env.DynamoDBTable,
			Key: {
				guid: guid,
			},
			// remove the trailing ',' from the update expression added by the forEach loop
			UpdateExpression: 'set ' + expression.slice(0, -1),
			ExpressionAttributeValues: values
		};

		console.log('Dynamo update: ', JSON.stringify(params, null, 2));
		await dynamo.update(params).promise();
		
		//START CUSTOM CODE TO UPDATE FIREBASE WHEN MEDIA CONVERSION WORKFLOW IS COMPLETE
		
		if (event.workflowStatus == "Complete") {
		
			
			console.log("Writing to Firebase")
			var https = require('https');
			const firebasePromise = new Promise(function(resolve, reject) {
				var body = JSON.stringify({
					experiencetestcat: "blahblah"
				})

				//USE HTTPS REQUEST TO TRIGGER FIREBASE CLOUD FUNCTION
				//SEND VIDEO METADATA TO FIREBASE USING THE CUSTOM URI PATH
				const thumbnailURLs = event.thumbNailUrl;
				var thumburlstring = '';
				console.log('thumnailList', thumbnailURLs)
				if (typeof thumbnailURLs !== 'undefined' && thumbnailURLs.length > 0) {
					console.log('defined thumb array', thumbnailURLs[0])
					thumburlstring = thumbnailURLs[0]; 
				}
				const captionString = event.caption;
				const enccap = captionString; //Encoded caption
				const thumburl = encodeURIComponent(thumburlstring);
				const hlsUrlString = event.hlsUrl;
				const vidurl = encodeURIComponent(hlsUrlString); //Encoded video URL
				var https = require('https');
				const vidCaption = 
				'/addVideo?caption=' + enccap
				+ '&userid=' + event.userid 
				+ '&thumburl=' + thumburl 
				+ '&storyid=' + event.storyid 
				+ '&draftcontentid=' + event.draftcontentid 
				+ '&vidurl=' + vidurl;
				var firebasePathURI = encodeURI(vidCaption); 
				var options = {
					host: 'us-central1-viewify-5c2f8.cloudfunctions.net',
					port: 443,
					path: firebasePathURI,
					method: 'GET'
				};

				var req = https.request(options, function(res) {
					console.log('STATUS CODE', res.statusCode);
					resolve(res.statusCode);
					res.on('data', function(d) {
						process.stdout.write(d);
					});
				});
				req.end(body);

				req.on('error', function(e) {
					reject(e);
				});
				

			})

			await firebasePromise;
			
		}
		
		
		//END FIREBASE CODE

		// Get updated data and reconst event data to return
		event.guid = guid;
	} catch (err) {
		console.log('Final Error',err);
		await error.handler(event, err);
		throw err;
	}
	return event;
};