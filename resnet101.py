from keras.applications import ResNet101
from keras import utils
from keras.applications.resnet import preprocess_input, decode_predictions
import numpy as np
import sys
model = ResNet101(weights='imagenet')

img_path = sys.argv[1]
save_path = sys.argv[2]
f = open(save_path,'w')
img = utils.load_img(img_path, target_size=(224, 224))
x = utils.img_to_array(img)
x = np.expand_dims(x, axis=0)
x = preprocess_input(x)

preds = model.predict(x)

print('Predicted:', decode_predictions(preds, top=3)[0], file=f)
