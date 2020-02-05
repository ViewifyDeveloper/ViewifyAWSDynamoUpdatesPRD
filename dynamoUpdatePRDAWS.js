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
			Bucket: "viewifyvideoprocessing-source-1vnu9luaobnpt",
			Key: event.srcVideo
		};
		
		//ADD PHOTO/VIDEO CAPTION TO EVENT METADATA
		const metadataPromise = new Promise(function(resolve, reject) {

			s3.headObject(mdparams, function(err, data) {
				if (err) {
					//Error occurred 
					var map1 = new Map(); //Create map of all metadata and return it from promise
					map1.set('error', true);
					reject(err);
					
				}
				else {

					var map1 = new Map(); //Create map of all metadata and return it from promise
          			map1.set('caption', data["Metadata"]["caption"]);	
          			map1.set('expid', data["Metadata"]["expid"]);
          			map1.set('priority', data["Metadata"]["priority"]);
          			map1.set('storyid', data["Metadata"]["storyid"]);
          			map1.set('userid', data["Metadata"]["userid"]);
          			map1.set('draftcontentid', data["Metadata"]["draftcontentid"]);
          			map1.set('appUpload', data["Metadata"]["appUpload"]); //Check if upload is coming from app versus migration
          			map1.set('error', false);
					//Successful response
					console.log(data);
					resolve(map1);
				} 
		
			});

		})
		
		let metadataMap = await metadataPromise;
		const hasError = metadataMap.get('error');
		console.log('Error parsing metadata: ', hasError)
		if (hasError) {
			return;
		}

		var isInAppUpload =  false;
		if (metadataMap.get('appUpload') == 'true') {
			isInAppUpload = true;
		}
		console.log('isInAppUpload', isInAppUpload);


		
		
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
				const captionString = metadataMap.get('caption');
				var enccap = captionString; //Encoded caption
				if (!enccap || enccap.length === 0 || enccap === "."){
					enccap = " "; //If caption is undefined, replace with single space
				}
				enccap = enccap.replace(/#/g,"%23"); //Hashtags need to be replaced, they are breaking URL params use the /g tag to replace them globally
				console.log('capstring', captionString)
				console.log('enccap', enccap)
				console.log('thumbURLToEncode', thumburlstring)
				const thumburl = encodeURIComponent(thumburlstring);
				console.log('encodedURL', thumburl)
				const hlsUrlString = event.hlsUrl;
				const vidurl = encodeURIComponent(hlsUrlString); //Encoded video URL
				var https = require('https');
				var firebaseCloudURL = '';
				if (isInAppUpload) {
					firebaseCloudURL = '/addVideo?caption=' + enccap
				+ '&userid=' + metadataMap.get('userid')
				+ '&thumburl=' + thumburl 
				+ '&storyid=' + metadataMap.get('storyid')
				+ '&draftcontentid=' + metadataMap.get('draftcontentid') 
				+ '&vidurl=' + vidurl;
				} else {
					firebaseCloudURL = '/addMigratedVideo?caption=' + enccap
				+ '&expid=' + metadataMap.get('expid')
				+ '&priority=' + metadataMap.get('priority')
				+ '&storyid=' + metadataMap.get('storyid')
				+ '&userid=' + metadataMap.get('userid')
				+ '&thumburl=' + thumburl
				+ '&vidurl=' + vidurl;
				}
				var firebasePathURI = encodeURI(firebaseCloudURL); 
				var options = {
					host: 'us-central1-viewifyproduction.cloudfunctions.net',
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
